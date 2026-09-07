"""Durable, scope-fenced history failure reports, one account per quantum."""

import logging

from workspace_zulip_bridge import history

LOG = logging.getLogger(__name__)
CAPTURE_TIMEOUT = "history_capture_write_timeout"
DIRECTORY_TIMEOUT = "history_directory_write_timeout"


class _EnqueueFailed(Exception):
    def __init__(self, scope, work):
        self.scope = scope
        self.work = work


def _scope_current(store, session, scope, configuration, generation, code):
    return (
        configuration is not None
        and (not configuration["directory_pending"] or code in {CAPTURE_TIMEOUT, DIRECTORY_TIMEOUT})
        and (
            code != DIRECTORY_TIMEOUT
            or configuration["directory_write_attempts"]
            >= history.DIRECTORY_WRITE_MAX_ATTEMPTS
        )
        and configuration["generation"] == generation
        and configuration["fingerprint"]
        == store.history_configuration_fingerprint(session, *scope)
    )


def record(session, item, code, *, restart_completed=False):
    """Share the failure transaction without expanding the source list."""
    # A terminal publication failure can coexist with captures (for example an
    # oversized source list). Keep that stronger error when a capture times out.
    session.execute(
        """INSERT INTO zulip_history_failure_reports
             (project_uuid,provider_realm_uuid,generation,safe_error_code)
           VALUES (%s,%s,%s,%s)
           ON CONFLICT(project_uuid,provider_realm_uuid) DO UPDATE SET
             generation=EXCLUDED.generation,safe_error_code=EXCLUDED.safe_error_code,
             after_account=NULL,complete=false,retry_at=now()
           WHERE (zulip_history_failure_reports.generation<>EXCLUDED.generation
              OR zulip_history_failure_reports.safe_error_code<>EXCLUDED.safe_error_code
              OR (%s AND zulip_history_failure_reports.complete))
             AND (NOT %s
               OR zulip_history_failure_reports.generation<>EXCLUDED.generation
               OR zulip_history_failure_reports.safe_error_code=EXCLUDED.safe_error_code)""",
        (
            *item["scope"][:2],
            item["generation"],
            code,
            restart_completed,
            code == CAPTURE_TIMEOUT,
        ),
    )


def carry_capture(session, generation):
    """Keep capture failures when only the publication generation changes."""
    session.execute(
        """INSERT INTO zulip_history_failure_reports AS report
             (project_uuid,provider_realm_uuid,generation,safe_error_code)
           SELECT scope.project_uuid,scope.provider_realm_uuid,scope.generation,%s
           FROM zulip_history_scopes AS scope WHERE scope.generation=%s
             AND EXISTS (
               SELECT 1 FROM jsonb_to_recordset(scope.sources)
                 AS source(account_uuid uuid,provider_chat_key text)
               JOIN zulip_backfill_jobs AS job
                 ON job.account_uuid=source.account_uuid
                AND job.provider_chat_key=source.provider_chat_key
               WHERE job.capture_timeout_pending)
           ON CONFLICT(project_uuid,provider_realm_uuid) DO UPDATE SET
             generation=EXCLUDED.generation,safe_error_code=EXCLUDED.safe_error_code,
             after_account=CASE WHEN report.safe_error_code=%s THEN report.after_account END,
             complete=CASE WHEN report.safe_error_code=%s THEN report.complete ELSE false END,
             retry_at=CASE WHEN report.safe_error_code=%s THEN report.retry_at ELSE now() END
           WHERE report.generation<>EXCLUDED.generation""",
        (
            CAPTURE_TIMEOUT,
            generation,
            CAPTURE_TIMEOUT,
            CAPTURE_TIMEOUT,
            CAPTURE_TIMEOUT,
        ),
    )


def carry_directory(session, generation):
    """Metadata-only revisions preserve a stopped directory pass and its report."""
    session.execute(
        """UPDATE zulip_history_failure_reports AS report SET generation=scope.generation
           FROM zulip_history_scopes AS scope
           WHERE report.project_uuid=scope.project_uuid AND report.provider_realm_uuid=scope.provider_realm_uuid
             AND scope.generation=%s AND scope.directory_pending AND scope.directory_write_attempts>=%s
             AND report.safe_error_code=%s""",
        (generation, history.DIRECTORY_WRITE_MAX_ATTEMPTS, DIRECTORY_TIMEOUT),
    )


def flush_once(store, account_report):
    """The callback must durably enqueue locally and return an acknowledgement."""
    try:
        return _flush_once(store, account_report)
    except _EnqueueFailed as failure:
        # The callback and cursor transaction has already rolled back. Delay
        # only that attempt, so failed reporting cannot starve healthy scopes.
        with store.transaction() as session:
            session.execute("SET LOCAL lock_timeout = '50ms'")
            session.execute("SET LOCAL statement_timeout = '500ms'")
            session.execute("LOCK TABLE desired_resources IN SHARE MODE")
            configuration = session.execute(
                "SELECT generation,fingerprint,directory_pending,directory_write_attempts FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR SHARE",
                failure.scope,
            ).fetchone()
            if _scope_current(
                store,
                session,
                failure.scope,
                configuration,
                failure.work["generation"],
                failure.work["safe_error_code"],
            ):
                session.execute(
                    """UPDATE zulip_history_failure_reports SET retry_at=now()+interval '30 seconds'
                       WHERE project_uuid=%s AND provider_realm_uuid=%s AND generation=%s
                         AND after_account IS NOT DISTINCT FROM %s::uuid
                         AND safe_error_code=%s AND NOT complete""",
                    (
                        *failure.scope,
                        failure.work["generation"],
                        failure.work["after_account"],
                        failure.work["safe_error_code"],
                    ),
                )
        LOG.warning("History account report enqueue failed; durable work will retry")
        return False


def _flush_once(store, account_report):
    if account_report is None:
        return False
    with store.transaction() as session:
        session.execute("SET LOCAL lock_timeout = '50ms'")
        session.execute("SET LOCAL statement_timeout = '500ms'")
        candidate = session.execute(
            """SELECT project_uuid,provider_realm_uuid,safe_error_code FROM zulip_history_failure_reports
               WHERE NOT complete AND retry_at<=now()
               ORDER BY retry_at,project_uuid,provider_realm_uuid LIMIT 1"""
        ).fetchone()
        if candidate is None:
            return False
        scope = (candidate["project_uuid"], candidate["provider_realm_uuid"])
        session.execute("LOCK TABLE desired_resources IN SHARE MODE")
        configuration = session.execute(
            "SELECT generation,fingerprint,directory_pending,directory_write_attempts FROM zulip_history_scopes WHERE project_uuid=%s AND provider_realm_uuid=%s FOR SHARE",
            scope,
        ).fetchone()
        locked_timeout = False
        if candidate["safe_error_code"] == CAPTURE_TIMEOUT:
            # Match capture's scope -> job -> report order. A successful save
            # cannot clear the last marker between validation and enqueue. One
            # pending job proves the scope error; other captures remain free.
            locked_timeout = bool(
                session.execute(
                    """SELECT job.account_uuid,job.provider_chat_key
                   FROM zulip_history_scopes AS scope
                   CROSS JOIN LATERAL jsonb_to_recordset(scope.sources)
                     AS source(account_uuid uuid,provider_chat_key text)
                   JOIN zulip_backfill_jobs AS job
                     ON job.account_uuid=source.account_uuid
                    AND job.provider_chat_key=source.provider_chat_key
                   WHERE scope.project_uuid=%s AND scope.provider_realm_uuid=%s
                     AND job.capture_timeout_pending
                   ORDER BY job.account_uuid,job.provider_chat_key LIMIT 1 FOR SHARE OF job""",
                    scope,
                ).fetchone()
            )
        work = session.execute(
            """SELECT * FROM zulip_history_failure_reports
               WHERE project_uuid=%s AND provider_realm_uuid=%s AND NOT complete
               FOR UPDATE SKIP LOCKED""",
            scope,
        ).fetchone()
        if work is None:
            return False
        if not _scope_current(
            store,
            session,
            scope,
            configuration,
            work["generation"],
            work["safe_error_code"],
        ):
            session.execute(
                "DELETE FROM zulip_history_failure_reports WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            return True
        if work["safe_error_code"] == CAPTURE_TIMEOUT and not locked_timeout:
            if session.execute(
                """SELECT EXISTS (
                 SELECT 1 FROM zulip_history_scopes AS scope
                 CROSS JOIN LATERAL jsonb_to_recordset(scope.sources)
                   AS source(account_uuid uuid,provider_chat_key text)
                 JOIN zulip_backfill_jobs AS job
                   ON job.account_uuid=source.account_uuid
                  AND job.provider_chat_key=source.provider_chat_key
                 WHERE scope.project_uuid=%s AND scope.provider_realm_uuid=%s
                   AND job.capture_timeout_pending
               ) AS present""",
                scope,
            ).fetchone()["present"]:
                # A new timeout committed after the marker-lock query, or
                # replaced different reporting work. Acquire it next quantum
                # instead of taking job locks after locking the report row.
                return False
            # Only successful capture or a real rebuild clears this marker;
            # a later provider error is not evidence of successful persistence.
            session.execute(
                "DELETE FROM zulip_history_failure_reports WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            return True
        if (configuration["directory_pending"] and work["safe_error_code"] != DIRECTORY_TIMEOUT) or not store.provider_is_enabled("zulip"):
            # A provider pause does not retire failed batches or their report
            # cursor. Recheck the same scope authority after resuming.
            session.execute(
                "UPDATE zulip_history_failure_reports SET retry_at=now()+interval '30 seconds' WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            return False
        source = session.execute(
            """SELECT DISTINCT source.account_uuid,source.account_generation
               FROM zulip_history_scopes AS scope
               CROSS JOIN LATERAL jsonb_to_recordset(scope.sources)
                 AS source(account_uuid uuid,account_generation bigint)
               WHERE scope.project_uuid=%s AND scope.provider_realm_uuid=%s
                 AND (%s::uuid IS NULL OR source.account_uuid>%s::uuid)
               ORDER BY source.account_uuid LIMIT 1""",
            (*scope, work["after_account"], work["after_account"]),
        ).fetchone()
        if source is None:
            session.execute(
                "UPDATE zulip_history_failure_reports SET complete=true WHERE project_uuid=%s AND provider_realm_uuid=%s",
                scope,
            )
            return True
        try:
            retained = account_report(
                str(source["account_uuid"]),
                "degraded",
                work["safe_error_code"],
                expected_generation=source["account_generation"],
            )
        except Exception as error:
            raise _EnqueueFailed(scope, work) from error
        if retained is True:
            session.execute(
                "UPDATE zulip_history_failure_reports SET after_account=%s,retry_at=now() WHERE project_uuid=%s AND provider_realm_uuid=%s",
                (source["account_uuid"], *scope),
            )
            return True
        session.execute(
            "UPDATE zulip_history_failure_reports SET retry_at=now()+interval '30 seconds' WHERE project_uuid=%s AND provider_realm_uuid=%s",
            scope,
        )
        return False
