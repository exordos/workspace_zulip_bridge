# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

"""Workspace bridge enrollment and desired external-account convergence."""

import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import secrets
import ssl
import tempfile
import typing
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID
from uuid import uuid4
from uuid import uuid5

import asyncpg
import httpx
import pyhpke
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric import x25519
from cryptography.x509.oid import ExtendedKeyUsageOID

from workspace_zulip_bridge.config import Settings
from workspace_zulip_bridge.stable_ids import canonical_endpoint
from workspace_zulip_bridge.stable_ids import stable_realm_uuid
from workspace_zulip_bridge.stable_ids import stable_user_uuid
from workspace_zulip_bridge.zulip_api import ZulipApiClient
from workspace_zulip_bridge.zulip_api import ZulipApiError

LOG = logging.getLogger(__name__)

_ENROLLMENT_CONTEXT = b"workspace-bridge-enrollment-v1\0"
_CA_CONTEXT = b"workspace-external-bridge-control-ca-v1\0"
_CREDENTIAL_INFO = b"workspace-external-credential-zulip-v1"
_CREDENTIAL_SCHEMA = "workspace.external-credential.zulip/v1"
_CREDENTIAL_ALGORITHM = "HPKE-v1-BASE-X25519-HKDF-SHA256-AES-256-GCM"
_RESOURCE_TYPES = ("custom_ca_bundle", "external_account")
_CERTIFICATE_RENEWAL_WINDOW = datetime.timedelta(days=7)
_CAPABILITIES = {
    name: {"revision": 1, "limits": {}}
    for name in (
        "messenger.chat_catalog",
        "messenger.message.delete",
        "messenger.message.edit",
        "messenger.message.read",
        "messenger.message.read.paging",
        "messenger.message.send",
        "messenger.membership.write",
        "messenger.notification.write",
        "messenger.reaction.write",
        "messenger.stream.rename",
        "messenger.topic.rename",
    )
}


class _DesiredResourceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(code)
        self.code = code
        self.safe_message = message


class _RetryableDesiredStateError(RuntimeError):
    pass


class WorkspaceControlWorker:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        assert settings.workspace_control_url is not None
        assert settings.workspace_control_bootstrap_url is not None
        assert settings.workspace_control_hostname is not None
        assert settings.workspace_realm_uuid is not None
        assert settings.workspace_bridge_instance_uuid is not None
        assert settings.workspace_enrollment_secret_file is not None
        self._pool = pool
        self._settings = settings
        self._state = settings.workspace_control_state_dir
        self._ca = self._state / "control-ca.pem"
        self._tls_key = self._state / "bridge.key"
        self._certificate = self._state / "bridge.crt"
        self._credential_key = self._state / "credential-x25519.key"
        self._request = self._state / "enrollment-request.json"
        self._cursor = self._state / "desired-state-cursor"
        self._last_heartbeat = 0.0
        self._enrollment_lock = asyncio.Lock()

    async def run(self) -> None:
        tasks = (
            asyncio.create_task(self._sync_loop(), name="workspace-control-sync"),
            asyncio.create_task(
                self._heartbeat_loop(),
                name="workspace-control-heartbeat",
            ),
        )
        try:
            completed, _ = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )
            next(iter(completed)).result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _sync_loop(self) -> None:
        attempt = 0
        while True:
            try:
                await self._ensure_enrolled()
                await self._sync_once()
                attempt = 0
                delay = self._settings.workspace_control_poll_seconds
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.warning(
                    "Workspace desired-state synchronization failed: error=%s",
                    type(error).__name__,
                )
                delay = _retry_delay(attempt)
                attempt += 1
            await asyncio.sleep(delay)

    async def _heartbeat_loop(self) -> None:
        attempt = 0
        while True:
            try:
                await self._ensure_enrolled()
                await self._heartbeat()
                attempt = 0
                delay = self._settings.workspace_control_poll_seconds
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.warning(
                    "Workspace control heartbeat failed: error=%s",
                    type(error).__name__,
                )
                delay = _retry_delay(attempt)
                attempt += 1
            await asyncio.sleep(delay)

    async def _ensure_enrolled(self) -> None:
        async with self._enrollment_lock:
            await self._ensure_enrolled_locked()

    async def _ensure_enrolled_locked(self) -> None:
        if all(
            path.is_file()
            for path in (
                self._ca,
                self._tls_key,
                self._certificate,
                self._credential_key,
                self._request,
            )
        ):
            if self._validate_certificate():
                try:
                    await self._renew_certificate()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # Renewal is opportunistic while the current certificate
                    # is still valid.  Keep desired-state polling and the
                    # heartbeat alive; the next pass will retry renewal.
                    LOG.warning(
                        "Workspace bridge certificate renewal deferred: error=%s",
                        type(error).__name__,
                    )
            return
        self._state.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not self._ca.is_file():
            await self._bootstrap_ca()
        request = self._load_or_create_request()
        assert self._settings.workspace_control_url is not None
        async with httpx.AsyncClient(
            base_url=self._settings.workspace_control_url,
            verify=str(self._ca),
            follow_redirects=False,
            timeout=10.0,
        ) as client:
            response = await client.post(
                "/v1/enrollments",
                json=request,
                headers={
                    "X-Workspace-Enrollment-Token": self._enrollment_secret(),
                },
            )
        response.raise_for_status()
        issuance = _object(response.json())
        certificate = self._validate_issuance(issuance, request)
        trust_bundle = issuance.get("trust_bundle_pem")
        if (
            not isinstance(trust_bundle, list)
            or not trust_bundle
            or not all(isinstance(value, str) for value in trust_bundle)
        ):
            raise ValueError("invalid enrollment trust bundle")
        _atomic_write(
            self._ca,
            "".join(trust_bundle).encode("ascii"),
            0o644,
        )
        _atomic_write(
            self._certificate,
            certificate.public_bytes(serialization.Encoding.PEM),
            0o600,
        )
        LOG.info("Workspace bridge enrollment completed")

    async def _bootstrap_ca(self) -> None:
        nonce = secrets.token_hex(32)
        hostname = self._settings.workspace_control_hostname
        instance_uuid = self._settings.workspace_bridge_instance_uuid
        assert hostname is not None
        assert instance_uuid is not None
        assert self._settings.workspace_control_bootstrap_url is not None
        async with httpx.AsyncClient(
            base_url=self._settings.workspace_control_bootstrap_url,
            follow_redirects=False,
            timeout=10.0,
        ) as client:
            response = await client.get(
                "/ca.crt",
                headers={"Content-Length": "0"},
                params={
                    "nonce": nonce,
                    "hostname": hostname,
                    "bridge_instance_uuid": str(instance_uuid),
                    "enrollment_generation": "1",
                },
            )
        response.raise_for_status()
        content = response.content
        length = response.headers.get("Content-Length")
        if length is None or int(length) != len(content) or not content:
            raise ValueError("invalid control CA response length")
        key = hashlib.sha256(
            _ENROLLMENT_CONTEXT + self._enrollment_secret().encode("utf-8")
        ).digest()
        message = b"\0".join(
            (
                _CA_CONTEXT[:-1],
                nonce.encode("ascii"),
                hostname.encode("utf-8"),
                str(instance_uuid).encode("ascii"),
                b"1",
                content,
            )
        )
        expected = hmac.new(key, message, hashlib.sha256).hexdigest()
        supplied = response.headers.get("X-Workspace-CA-HMAC-SHA256", "")
        if not hmac.compare_digest(expected, supplied):
            raise ValueError("control CA authentication failed")
        ssl.create_default_context(cadata=content.decode("ascii"))
        _atomic_write(self._ca, content, 0o644)

    def _load_or_create_request(self) -> dict[str, Any]:
        if self._request.is_file():
            return _object(json.loads(self._request.read_text(encoding="utf-8")))
        tls_key = ec.generate_private_key(ec.SECP256R1())
        tls_key_pem = tls_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        credential_key = x25519.X25519PrivateKey.generate()
        credential_key_pem = credential_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([]))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(self._identity_uri())]
                ),
                critical=False,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False,
            )
            .sign(tls_key, hashes.SHA256())
        )
        public_key = credential_key.public_key().public_bytes(
            serialization.Encoding.Raw,
            serialization.PublicFormat.Raw,
        )
        request = {
            "request_uuid": str(uuid4()),
            "enrollment_generation": 1,
            "realm_uuid": str(self._settings.workspace_realm_uuid),
            "provider_kind": "zulip",
            "bridge_instance_uuid": str(self._settings.workspace_bridge_instance_uuid),
            "csr_pem": csr.public_bytes(serialization.Encoding.PEM).decode("ascii"),
            "encryption_public_key": {
                "key_uuid": str(uuid4()),
                "algorithm": "X25519",
                "public_key": _encode(public_key),
            },
        }
        _atomic_write(self._tls_key, tls_key_pem, 0o600)
        _atomic_write(self._credential_key, credential_key_pem, 0o600)
        _atomic_write(
            self._request,
            json.dumps(request, sort_keys=True).encode("utf-8"),
            0o600,
        )
        return request

    def _validate_issuance(
        self,
        issuance: Mapping[str, Any],
        request: Mapping[str, Any],
    ) -> x509.Certificate:
        if issuance.get("request_uuid") != request["request_uuid"]:
            raise ValueError("enrollment request UUID mismatch")
        identity = _object(issuance["identity"])
        if (
            identity.get("realm_uuid") != str(self._settings.workspace_realm_uuid)
            or identity.get("provider_kind") != "zulip"
            or identity.get("bridge_instance_uuid")
            != str(self._settings.workspace_bridge_instance_uuid)
            or identity.get("identity_generation") != 1
            or identity.get("uri_san") != self._identity_uri()
        ):
            raise ValueError("enrollment identity mismatch")
        value = issuance.get("certificate_pem")
        if not isinstance(value, str):
            raise ValueError("invalid enrollment certificate")
        certificate = x509.load_pem_x509_certificate(value.encode("ascii"))
        key = serialization.load_pem_private_key(
            self._tls_key.read_bytes(), password=None
        )
        key = typing.cast(ec.EllipticCurvePrivateKey, key)
        if _public_key_bytes(certificate.public_key()) != _public_key_bytes(
            key.public_key()
        ):
            raise ValueError("enrollment certificate key mismatch")
        names = certificate.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value.get_values_for_type(x509.UniformResourceIdentifier)
        if names != [self._identity_uri()]:
            raise ValueError("enrollment certificate identity mismatch")
        return certificate

    def _validate_certificate(self) -> bool:
        certificate = x509.load_pem_x509_certificate(self._certificate.read_bytes())
        key = serialization.load_pem_private_key(
            self._tls_key.read_bytes(), password=None
        )
        if _public_key_bytes(certificate.public_key()) != _public_key_bytes(
            key.public_key()
        ):
            raise ValueError("control certificate key mismatch")
        now = datetime.datetime.now(datetime.UTC)
        if (
            not certificate.not_valid_before_utc
            <= now
            < certificate.not_valid_after_utc
        ):
            raise ValueError("control certificate is not currently valid")
        return certificate.not_valid_after_utc - now <= _CERTIFICATE_RENEWAL_WINDOW

    def _identity_uri(self) -> str:
        return (
            "https://schemas.genesis-corporation.ru/workspace/external-bridge/v1/"
            f"realms/{self._settings.workspace_realm_uuid}/providers/zulip/"
            f"instances/{self._settings.workspace_bridge_instance_uuid}/"
            "generations/1"
        )

    def _enrollment_secret(self) -> str:
        path = self._settings.workspace_enrollment_secret_file
        assert path is not None
        value = path.read_text(encoding="utf-8").strip()
        if not value or any(character in value for character in "\r\n\0"):
            raise ValueError("invalid Workspace enrollment secret")
        return value

    def _client(self) -> httpx.AsyncClient:
        context = ssl.create_default_context(cafile=str(self._ca))
        context.load_cert_chain(str(self._certificate), str(self._tls_key))
        assert self._settings.workspace_control_url is not None
        return httpx.AsyncClient(
            base_url=self._settings.workspace_control_url,
            verify=context,
            follow_redirects=False,
            timeout=30.0,
            headers={"Accept": "application/json"},
        )

    async def _sync_once(self) -> None:
        cursor = self._load_cursor()
        if cursor is None:
            await self._snapshot()
            return
        async with self._client() as client:
            response = await client.get(
                "/v1/desired-state/changes",
                headers={"Content-Length": "0"},
                params={
                    "cursor": cursor,
                    "resource_types": ",".join(_RESOURCE_TYPES),
                    "page_limit": "200",
                },
            )
        if response.status_code == 410:
            self._cursor.unlink(missing_ok=True)
            await self._snapshot()
            return
        response.raise_for_status()
        batch = _object(response.json())
        if batch.get("current_cursor") != cursor:
            raise ValueError("desired-state cursor mismatch")
        changes = batch.get("changes")
        if not isinstance(changes, list):
            raise ValueError("invalid desired-state change batch")
        ordered = sorted(
            (_object(change) for change in changes),
            key=lambda item: item.get("resource_type") != "custom_ca_bundle",
        )
        retry_required = False
        for change in ordered:
            retry_required = await self._apply_change(change) or retry_required
        if retry_required:
            raise _RetryableDesiredStateError
        next_cursor = batch.get("next_cursor")
        if not isinstance(next_cursor, str) or not next_cursor:
            raise ValueError("invalid desired-state next cursor")
        _atomic_write(self._cursor, next_cursor.encode("ascii"), 0o600)

    async def _snapshot(self) -> None:
        async with self._client() as client:
            response = await client.post(
                "/v1/desired-state/snapshots",
                json={
                    "request_uuid": str(uuid4()),
                    "resource_types": list(_RESOURCE_TYPES),
                },
            )
            response.raise_for_status()
            session = _object(response.json())
            token = session.get("snapshot_token")
            anchor = session.get("anchor_cursor")
            if not isinstance(token, str) or not isinstance(anchor, str):
                raise ValueError("invalid desired-state snapshot session")
            page_cursor: str | None = None
            resources: list[dict[str, Any]] = []
            while True:
                params = {"page_limit": "200"}
                if page_cursor is not None:
                    params["page_cursor"] = page_cursor
                response = await client.get(
                    f"/v1/desired-state/snapshots/{token}/pages",
                    headers={"Content-Length": "0"},
                    params=params,
                )
                response.raise_for_status()
                page = _object(response.json())
                values = page.get("resources")
                if not isinstance(values, list):
                    raise ValueError("invalid desired-state snapshot page")
                resources.extend(_object(value) for value in values)
                next_cursor = page.get("next_page_cursor")
                if next_cursor is None:
                    break
                if not isinstance(next_cursor, str) or next_cursor == page_cursor:
                    raise ValueError("invalid desired-state page cursor")
                page_cursor = next_cursor
        ordered = sorted(
            resources,
            key=lambda item: item.get("resource_type") != "custom_ca_bundle",
        )
        account_uuids: set[UUID] = set()
        retry_required = False
        for resource in ordered:
            if resource.get("resource_type") == "external_account":
                account_uuids.add(UUID(str(resource["uuid"])))
            retry_required = await self._apply_resource(resource) or retry_required
        await self._disable_absent_accounts(account_uuids)
        if retry_required:
            raise _RetryableDesiredStateError
        _atomic_write(self._cursor, anchor.encode("ascii"), 0o600)
        LOG.info(
            "Workspace desired-state snapshot applied: resources=%d accounts=%d",
            len(resources),
            len(account_uuids),
        )

    async def _apply_change(self, change: Mapping[str, Any]) -> bool:
        resource_type = change.get("resource_type")
        if resource_type not in _RESOURCE_TYPES:
            return False
        if change.get("operation") == "delete":
            if resource_type == "external_account":
                await self._disable_account(UUID(str(change["resource_uuid"])))
            elif resource_type == "custom_ca_bundle":
                self._remove_zulip_ca()
            return False
        if change.get("operation") != "upsert":
            raise ValueError("invalid desired-state operation")
        return await self._apply_resource(_object(change["resource"]))

    async def _apply_resource(self, resource: Mapping[str, Any]) -> bool:
        resource_type = resource.get("resource_type")
        try:
            if resource_type == "custom_ca_bundle":
                self._apply_zulip_ca(resource)
                status = "ready"
            elif resource_type == "external_account":
                await self._apply_account(resource)
                status = (
                    "live_ready"
                    if resource.get("synchronization_enabled") is True
                    else "suspended"
                )
            else:
                return False
        except ZulipApiError as error:
            status = (
                "auth_required" if error.status_code in {401, 403} else "disconnected"
            )
            await self._report_observed(
                resource,
                status,
                safe_error={
                    "code": status,
                    "message": (
                        "Zulip authentication failed."
                        if status == "auth_required"
                        else "Zulip is temporarily unavailable."
                    ),
                    "retryable": error.retryable,
                },
            )
            return error.retryable
        except httpx.HTTPError:
            await self._report_observed(
                resource,
                "disconnected",
                safe_error={
                    "code": "provider_unreachable",
                    "message": "Zulip is temporarily unavailable.",
                    "retryable": True,
                },
            )
            return True
        except _DesiredResourceError as error:
            await self._report_observed(
                resource,
                "failed",
                safe_error={
                    "code": error.code,
                    "message": error.safe_message,
                    "retryable": False,
                },
            )
            return False
        except ValueError:
            await self._report_observed(
                resource,
                "failed",
                safe_error={
                    "code": "invalid_desired_resource",
                    "message": "The desired resource could not be applied.",
                    "retryable": False,
                },
            )
            return False
        await self._report_observed(resource, status)
        return False

    async def _report_observed(
        self,
        resource: Mapping[str, Any],
        status: str,
        *,
        safe_error: dict[str, object] | None = None,
    ) -> None:
        resource_type = str(resource["resource_type"])
        resource_uuid = UUID(str(resource["uuid"]))
        generation = int(resource["generation"])
        bridge_uuid = self._settings.workspace_bridge_instance_uuid
        assert bridge_uuid is not None
        report_uuid = uuid5(
            bridge_uuid,
            f"observed:{resource_type}:{resource_uuid}:{generation}:{status}",
        )
        observed_at = _utc_now()
        report = {
            "report_uuid": str(report_uuid),
            "resource_type": resource_type,
            "resource_uuid": str(resource_uuid),
            "observed_generation": generation,
            "status": status,
            "progress": {
                "phase": "live" if status == "live_ready" else status,
                "completed": 1,
                "total": 1,
                "last_progress_at": observed_at,
            },
            "safe_error": safe_error,
            "observed_at": observed_at,
        }
        async with self._client() as client:
            response = await client.post(
                "/v1/observed-state/reports",
                json={"reports": [report]},
            )
        response.raise_for_status()
        payload = _object(response.json())
        results = payload.get("results")
        if not isinstance(results, list) or len(results) != 1:
            raise ValueError("invalid observed-state response")
        result = _object(results[0])
        if result.get("report_uuid") != str(report_uuid) or result.get(
            "status"
        ) not in {"applied", "duplicate", "stale"}:
            raise ValueError("observed-state report was rejected")

    def _apply_zulip_ca(self, resource: Mapping[str, Any]) -> None:
        if resource.get("provider_kind") != "zulip":
            return
        values = resource.get("certificates_pem")
        if (
            not isinstance(values, list)
            or not values
            or not all(isinstance(value, str) for value in values)
        ):
            raise ValueError("invalid Zulip CA bundle")
        content = "".join(values).encode("ascii")
        if hashlib.sha256(content).hexdigest() != resource.get("sha256"):
            raise ValueError("Zulip CA bundle hash mismatch")
        for value in values:
            certificate = x509.load_pem_x509_certificate(value.encode("ascii"))
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            if not constraints.ca:
                raise ValueError("Zulip trust bundle contains a leaf certificate")
        path = self._settings.zulip_ca_materialization_file
        assert path is not None
        _atomic_write(path, content, 0o644)

    def _remove_zulip_ca(self) -> None:
        path = self._settings.zulip_ca_materialization_file
        if path is not None:
            path.unlink(missing_ok=True)

    async def _apply_account(self, resource: Mapping[str, Any]) -> None:
        account_uuid = UUID(str(resource["uuid"]))
        generation = int(resource["generation"])
        owner_uuid = UUID(str(resource["owner_user_uuid"]))
        if resource.get("synchronization_enabled") is not True:
            await self._disable_account(account_uuid)
            return
        settings = _object(resource["settings"])
        project_uuid = UUID(str(settings["default_project_id"]))
        configured_project_uuid = self._settings.workspace_project_id
        if (
            configured_project_uuid is not None
            and project_uuid != configured_project_uuid
        ):
            raise _DesiredResourceError(
                "workspace_project_mismatch",
                "The external account belongs to another Workspace project.",
            )
        envelope = resource.get("credential_envelope")
        if not isinstance(envelope, Mapping):
            raise ValueError("enabled external account has no credential")
        credentials = self._decrypt_credentials(
            account_uuid,
            owner_uuid,
            generation,
            envelope,
        )
        endpoint = canonical_endpoint(str(settings["server_url"]))
        if endpoint != canonical_endpoint(credentials["server_url"]):
            raise ValueError("external account endpoint mismatch")
        identity = await asyncio.to_thread(
            self._read_zulip_identity,
            endpoint,
            credentials["email"],
            credentials["api_key"],
        )
        provider_uuid = self._settings.workspace_provider_uuid
        assert provider_uuid is not None
        realm_uuid = stable_realm_uuid(endpoint)
        user_uuid = stable_user_uuid(endpoint, identity.user_id)
        async with self._pool.acquire() as connection, connection.transaction():
            other_realm = await connection.fetchval(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_realms
                WHERE workspace_provider_uuid = $1 AND uuid <> $2
                LIMIT 1
                """,
                provider_uuid,
                realm_uuid,
            )
            if other_realm is not None:
                raise _DesiredResourceError(
                    "provider_realm_mismatch",
                    "The bridge is already connected to another Zulip realm.",
                )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_realms (
                    uuid, identity_key, endpoint, workspace_project_id,
                    workspace_provider_uuid
                ) VALUES ($1, $2, $2, $3, $4)
                ON CONFLICT (uuid) DO UPDATE SET
                    workspace_project_id = EXCLUDED.workspace_project_id,
                    workspace_provider_uuid = EXCLUDED.workspace_provider_uuid,
                    updated_at = clock_timestamp()
                """,
                realm_uuid,
                endpoint,
                project_uuid,
                provider_uuid,
            )
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_users (
                    uuid, realm_uuid, zulip_user_id, login, full_name, role,
                    workspace_user_uuid
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (uuid) DO UPDATE SET
                    login = EXCLUDED.login, full_name = EXCLUDED.full_name,
                    role = EXCLUDED.role, disabled = false,
                    workspace_user_uuid = EXCLUDED.workspace_user_uuid,
                    updated_at = clock_timestamp()
                """,
                user_uuid,
                realm_uuid,
                identity.user_id,
                credentials["email"],
                identity.full_name,
                identity.role,
                owner_uuid,
            )
            existing = await connection.fetchrow(
                """
                SELECT uuid
                FROM workspace_zulip_bridge.zulip_connections
                WHERE external_account_uuid = $1
                   OR (realm_uuid = $2 AND login = $3)
                ORDER BY (external_account_uuid = $1) DESC
                LIMIT 1
                FOR UPDATE
                """,
                account_uuid,
                realm_uuid,
                credentials["email"],
            )
            connection_uuid = account_uuid if existing is None else existing["uuid"]
            await connection.execute(
                """
                INSERT INTO workspace_zulip_bridge.zulip_connections (
                    uuid, external_account_uuid, owner_workspace_user_uuid,
                    desired_generation, realm_uuid, zulip_user_uuid, login,
                    api_key, sync_enabled
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, TRUE)
                ON CONFLICT (uuid) DO UPDATE SET
                    external_account_uuid = EXCLUDED.external_account_uuid,
                    owner_workspace_user_uuid = EXCLUDED.owner_workspace_user_uuid,
                    desired_generation = EXCLUDED.desired_generation,
                    realm_uuid = EXCLUDED.realm_uuid,
                    zulip_user_uuid = EXCLUDED.zulip_user_uuid,
                    login = EXCLUDED.login, api_key = EXCLUDED.api_key,
                    sync_enabled = TRUE, updated_at = clock_timestamp()
                """,
                connection_uuid,
                account_uuid,
                owner_uuid,
                generation,
                realm_uuid,
                user_uuid,
                credentials["email"],
                credentials["api_key"],
            )
        LOG.info("Workspace external account activated: account_uuid=%s", account_uuid)

    def _read_zulip_identity(self, endpoint: str, email: str, api_key: str) -> Any:
        client = ZulipApiClient(
            endpoint,
            email,
            api_key,
            ca_file=self._settings.effective_zulip_ca_file,
            connect_timeout_seconds=self._settings.zulip_connect_timeout_seconds,
            default_longpoll_timeout_seconds=(
                self._settings.zulip_default_longpoll_timeout_seconds
            ),
            idle_queue_timeout_seconds=self._settings.zulip_idle_queue_timeout_seconds,
            chat_fill_timeout_seconds=self._settings.zulip_chat_fill_timeout_seconds,
            message_page_size=self._settings.zulip_message_page_size,
        )
        try:
            return client.get_own_user()
        finally:
            client.close()

    def _decrypt_credentials(
        self,
        account_uuid: UUID,
        owner_uuid: UUID,
        generation: int,
        envelope: Mapping[str, Any],
    ) -> dict[str, str]:
        if (
            envelope.get("schema") != _CREDENTIAL_SCHEMA
            or envelope.get("algorithm") != _CREDENTIAL_ALGORITHM
        ):
            raise ValueError("unsupported external credential envelope")
        request = _object(json.loads(self._request.read_text(encoding="utf-8")))
        public_key = _object(request["encryption_public_key"])
        expected = {
            "realm_uuid": str(self._settings.workspace_realm_uuid),
            "provider_kind": "zulip",
            "bridge_instance_uuid": str(self._settings.workspace_bridge_instance_uuid),
            "identity_generation": 1,
            "credential_key_uuid": str(public_key["key_uuid"]),
            "account_uuid": str(account_uuid),
            "owner_user_uuid": str(owner_uuid),
            "account_generation": generation,
            "schema": _CREDENTIAL_SCHEMA,
            "algorithm": _CREDENTIAL_ALGORITHM,
        }
        associated_data = _object(envelope["associated_data"])
        if associated_data != expected:
            raise ValueError("external credential associated data mismatch")
        private_key = pyhpke.KEMKey.from_pem(self._credential_key.read_bytes())
        suite = pyhpke.CipherSuite.new(
            pyhpke.KEMId.DHKEM_X25519_HKDF_SHA256,
            pyhpke.KDFId.HKDF_SHA256,
            pyhpke.AEADId.AES256_GCM,
        )
        recipient = suite.create_recipient_context(
            _decode(str(envelope["encapsulated_key"])),
            private_key,
            info=_CREDENTIAL_INFO,
        )
        plaintext = recipient.open(
            _decode(str(envelope["ciphertext"])),
            aad=json.dumps(
                associated_data,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        value = _object(json.loads(plaintext.decode("utf-8")))
        if set(value) != {"server_url", "email", "api_key"} or not all(
            isinstance(value[key], str) and value[key]
            for key in ("server_url", "email", "api_key")
        ):
            raise ValueError("invalid decrypted Zulip credential")
        return {
            "server_url": value["server_url"],
            "email": value["email"],
            "api_key": value["api_key"],
        }

    async def _disable_account(self, account_uuid: UUID) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_connections
            SET sync_enabled = false, queue_id = NULL, last_event_id = NULL,
                updated_at = clock_timestamp()
            WHERE external_account_uuid = $1
            """,
            account_uuid,
        )

    async def _disable_absent_accounts(self, account_uuids: set[UUID]) -> None:
        await self._pool.execute(
            """
            UPDATE workspace_zulip_bridge.zulip_connections
            SET sync_enabled = false, queue_id = NULL, last_event_id = NULL,
                updated_at = clock_timestamp()
            WHERE external_account_uuid IS NOT NULL
              AND NOT (external_account_uuid = ANY($1::uuid[]))
            """,
            list(account_uuids),
        )

    async def _heartbeat(self) -> None:
        now = asyncio.get_running_loop().time()
        if now - self._last_heartbeat < 10.0:
            return
        async with self._client() as client:
            response = await client.put(
                "/v1/bridge-instances/self/heartbeat",
                json={
                    "heartbeat_uuid": str(uuid4()),
                    "client_timestamp": _utc_now(),
                    "image_version": "v3",
                    "provider_kind": "zulip",
                    "capabilities": _CAPABILITIES,
                    "blocked_batch": None,
                },
            )
        response.raise_for_status()
        payload = _object(response.json())
        migration = _object(payload.get("ca_migration", {}))
        if migration.get("renewal_required") is True:
            await self._renew_certificate()
        self._last_heartbeat = now

    async def _renew_certificate(self) -> None:
        key = serialization.load_pem_private_key(
            self._tls_key.read_bytes(), password=None
        )
        key = typing.cast(ec.EllipticCurvePrivateKey, key)
        request_uuid = str(uuid4())
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(x509.Name([]))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.UniformResourceIdentifier(self._identity_uri())]
                ),
                critical=False,
            )
            .add_extension(
                x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
                critical=False,
            )
            .sign(key, hashes.SHA256())
        )
        request = {
            "request_uuid": request_uuid,
            "csr_pem": csr.public_bytes(serialization.Encoding.PEM).decode("ascii"),
        }
        async with self._client() as client:
            response = await client.post("/v1/certificate-renewals", json=request)
        response.raise_for_status()
        issuance = _object(response.json())
        certificate = self._validate_issuance(issuance, request)
        trust_bundle = issuance.get("trust_bundle_pem")
        if (
            not isinstance(trust_bundle, list)
            or not trust_bundle
            or not all(isinstance(value, str) for value in trust_bundle)
        ):
            raise ValueError("invalid renewal trust bundle")
        _atomic_write(self._ca, "".join(trust_bundle).encode("ascii"), 0o644)
        _atomic_write(
            self._certificate,
            certificate.public_bytes(serialization.Encoding.PEM),
            0o600,
        )
        LOG.info("Workspace bridge certificate renewed")

    def _load_cursor(self) -> str | None:
        if not self._cursor.is_file():
            return None
        value = self._cursor.read_text(encoding="ascii").strip()
        return value or None


def _utc_now() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat().replace("+00:00", "Z")


def _retry_delay(attempt: int) -> float:
    maximum = min(60.0, float(2 ** min(attempt, 6)))
    return maximum * 0.5 + secrets.randbelow(1000) / 1000 * maximum * 0.5


def _public_key_bytes(key: Any) -> bytes:
    return bytes(
        key.public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )


def _object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("expected a JSON object")
    return value


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _atomic_write(path: Path, content: bytes, mode: int) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
