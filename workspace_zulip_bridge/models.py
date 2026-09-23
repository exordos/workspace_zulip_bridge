# Copyright 2026 Genesis Corporation
# Licensed under the Apache License, Version 2.0 (the "License").

from dataclasses import dataclass
from dataclasses import field
from uuid import UUID


@dataclass(frozen=True, slots=True)
class ExternalAccount:
    uuid: UUID
    owner_workspace_user_uuid: UUID
    desired_generation: int
    workspace_project_id: UUID
    endpoint: str
    login: str
    api_key: str = field(repr=False)
    queue_id: str | None = None
    last_event_id: int | None = None

    def connection_signature(self) -> tuple[object, ...]:
        return (
            self.desired_generation,
            self.endpoint,
            self.login,
            self.api_key,
        )


@dataclass(frozen=True, slots=True)
class RegisteredQueue:
    queue_id: str
    last_event_id: int
    longpoll_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class WorkspaceEventCursor:
    epoch_generation: UUID | None
    last_epoch_version: int
