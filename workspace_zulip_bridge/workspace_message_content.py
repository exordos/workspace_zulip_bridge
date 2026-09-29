# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Serialize derived Workspace message content updates."""

from __future__ import annotations

import re
from collections.abc import Mapping
from collections.abc import Sequence
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from workspace_zulip_bridge.message_history import message_content_hash

_WORKSPACE_URN = re.compile(r"^urn:(?:file|image|video):[0-9a-f-]{36}$")


def replace_source_file_urn(content: str, source_uuid: UUID, target_urn: str) -> str:
    """Replace only the bridge's unresolved source-file placeholder."""

    if _WORKSPACE_URN.fullmatch(target_urn) is None:
        raise ValueError("invalid Workspace file URN")
    return content.replace(f"urn:file:{source_uuid}", target_urn)


async def preserve_finalized_file_references(
    connection: asyncpg.Connection | PoolConnectionProxy,
    message_uuids: Sequence[UUID],
    *,
    additional_references: Mapping[UUID, str] | None = None,
) -> list[UUID]:
    """Apply committed and just-finalized file URNs under message row locks.

    Every writer that can replace ``workspace_content`` calls this before its
    transaction commits.  A file finalizer passes its not-yet-committed URN as
    an additional reference.  This makes either transaction order converge to
    the same content without retaining a stale read/modify/write snapshot.
    """

    unique_message_uuids = sorted(set(message_uuids))
    if not unique_message_uuids:
        return []
    rows = await connection.fetch(
        """
        SELECT message.uuid, message.workspace_content,
               message.sender_user_uuid, stream.chat_key,
               topic.name AS topic_name, message.created_at
        FROM workspace_zulip_bridge.zulip_messages AS message
        JOIN workspace_zulip_bridge.zulip_streams AS stream
          ON stream.uuid = message.zulip_stream_uuid
        LEFT JOIN workspace_zulip_bridge.zulip_topics AS topic
          ON topic.uuid = message.topic_uuid
        WHERE message.uuid = ANY($1::uuid[])
          AND message.workspace_content IS NOT NULL
        ORDER BY message.uuid
        FOR UPDATE OF message
        """,
        unique_message_uuids,
    )
    if not rows:
        return []
    locked_message_uuids = [UUID(str(row["uuid"])) for row in rows]
    reference_rows = await connection.fetch(
        """
        SELECT link.message_uuid, link.file_uuid, projection.workspace_urn
        FROM workspace_zulip_bridge.zulip_message_files AS link
        JOIN workspace_zulip_bridge.zulip_messages AS message
          ON message.uuid = link.message_uuid
        JOIN workspace_zulip_bridge.workspace_file_projections AS projection
          ON projection.file_uuid = link.file_uuid
         AND projection.zulip_stream_uuid = message.zulip_stream_uuid
         AND projection.processing_status = 'finalized'
        WHERE link.message_uuid = ANY($1::uuid[])
        ORDER BY link.message_uuid, link.position, link.file_uuid
        """,
        locked_message_uuids,
    )
    references: dict[UUID, list[tuple[UUID, str]]] = {}
    for reference in reference_rows:
        references.setdefault(UUID(str(reference["message_uuid"])), []).append(
            (
                UUID(str(reference["file_uuid"])),
                str(reference["workspace_urn"]),
            )
        )
    if additional_references:
        linked_rows = await connection.fetch(
            """
            SELECT message_uuid, file_uuid
            FROM workspace_zulip_bridge.zulip_message_files
            WHERE message_uuid = ANY($1::uuid[])
              AND file_uuid = ANY($2::uuid[])
            ORDER BY message_uuid, position, file_uuid
            """,
            locked_message_uuids,
            list(additional_references),
        )
        for linked in linked_rows:
            references.setdefault(UUID(str(linked["message_uuid"])), []).append(
                (
                    UUID(str(linked["file_uuid"])),
                    additional_references[UUID(str(linked["file_uuid"]))],
                )
            )

    changed: list[tuple[UUID, str, bytes]] = []
    for row in rows:
        message_uuid = UUID(str(row["uuid"]))
        current = str(row["workspace_content"])
        projected = current
        seen: set[UUID] = set()
        for source_uuid, workspace_urn in references.get(message_uuid, ()):
            if source_uuid in seen:
                continue
            seen.add(source_uuid)
            projected = replace_source_file_urn(projected, source_uuid, workspace_urn)
        if projected == current:
            continue
        changed.append(
            (
                message_uuid,
                projected,
                message_content_hash(
                    sender_user_uuid=UUID(str(row["sender_user_uuid"])),
                    chat_key=str(row["chat_key"]),
                    topic_name=(
                        None if row["topic_name"] is None else str(row["topic_name"])
                    ),
                    content=projected,
                    sent_at=int(row["created_at"].timestamp()),
                ),
            )
        )
    if changed:
        await connection.executemany(
            """
            UPDATE workspace_zulip_bridge.zulip_messages
            SET workspace_content = $2, content_hash = $3,
                source_updated_at = GREATEST(source_updated_at, clock_timestamp()),
                updated_at = clock_timestamp()
            WHERE uuid = $1
            """,
            changed,
        )
    return [message_uuid for message_uuid, _, _ in changed]
