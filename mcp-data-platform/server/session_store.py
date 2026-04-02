from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Lock
from uuid import uuid4


@dataclass(frozen=True)
class SessionRecord:
    session_token: str
    agent_id: str
    org_id: str
    created_at: datetime
    expires_at: datetime


class SessionStore:
    def __init__(self, ttl_hours: int = 8) -> None:
        self._ttl = timedelta(hours=ttl_hours)
        self._lock = Lock()
        self._sessions: dict[str, SessionRecord] = {}

    def create_session(self, agent_id: str, org_id: str) -> SessionRecord:
        now = datetime.now(UTC)
        record = SessionRecord(
            session_token=f"sess_{uuid4().hex}",
            agent_id=agent_id,
            org_id=org_id,
            created_at=now,
            expires_at=now + self._ttl,
        )
        with self._lock:
            self._sessions[record.session_token] = record
        return record

    def validate(self, session_token: str) -> SessionRecord:
        with self._lock:
            record = self._sessions.get(session_token)
            if record is None:
                raise KeyError("Invalid session token.")
            if record.expires_at <= datetime.now(UTC):
                del self._sessions[session_token]
                raise KeyError("Session token expired.")
            return record
