"""Core sessionization and revision logic.

Write operations for one user are serialized by an in-process lock and each
runs inside a single SQLite transaction. Any failure (including the
test-injected :data:`FAIL_BEFORE_COMMIT`) rolls the whole transaction back,
so the previous revision remains the latest visible one.
"""
from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    EPOCH,
    Event,
    GroupedSession,
    Revision,
    RevisionChange,
    SessionGrouping,
    SessionSnapshot,
    UserConfig,
    to_utc,
    utc_now,
)

# Test hook: when True, the next write that would create a revision raises
# before commit, exercising the "publish failure keeps the old revision" rule.
FAIL_BEFORE_COMMIT = False


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


_user_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def user_lock(user_id: str) -> threading.Lock:
    with _locks_guard:
        lock = _user_locks.get(user_id)
        if lock is None:
            lock = threading.Lock()
            _user_locks[user_id] = lock
        return lock


def get_or_create_user(db: Session, user_id: str, idle_gap_seconds: int | None = None) -> UserConfig:
    cfg = db.get(UserConfig, user_id)
    if cfg is None:
        cfg = UserConfig(
            user_id=user_id,
            idle_gap_seconds=idle_gap_seconds or 600,
            watermark=EPOCH,
        )
        db.add(cfg)
        db.flush()
    return cfg


def set_idle_gap(db: Session, user_id: str, idle_gap_seconds: int) -> UserConfig:
    cfg = get_or_create_user(db, user_id)
    cfg.idle_gap_seconds = idle_gap_seconds
    db.flush()
    return cfg


def _session_key(event_ids: list[str]) -> str:
    digest = hashlib.sha256(",".join(event_ids).encode("utf-8")).hexdigest()
    return digest[:16]


def sessionize(events: list[Event], idle_gap_seconds: int) -> list[dict]:
    """Group events ordered by (event_time, event_id) into sessions.

    A new session starts whenever the gap between consecutive events is
    strictly greater than the user's idle interval; equal-to-gap events stay
    in the same session (gap <= idle interval means connected).
    """
    ordered = sorted(events, key=lambda e: (e.event_time, e.event_id))
    sessions: list[dict] = []
    current: dict | None = None
    gap = timedelta(seconds=idle_gap_seconds)

    for ev in ordered:
        if current is None or ev.event_time - current["end"] > gap:
            current = {
                "session_key": "",
                "start": ev.event_time,
                "end": ev.event_time,
                "event_ids": [ev.event_id],
            }
            sessions.append(current)
        else:
            current["end"] = ev.event_time
            current["event_ids"].append(ev.event_id)

    for sess in sessions:
        sess["session_key"] = _session_key(sess["event_ids"])
    return sessions


def classify_sessions(
    sessions: list[dict], watermark: datetime, idle_gap_seconds: int
) -> list[dict]:
    """Publish only sessions whose end is earlier than watermark - gap."""
    threshold = watermark - timedelta(seconds=idle_gap_seconds)
    for sess in sessions:
        sess["status"] = "published" if sess["end"] < threshold else "pending"
    return sessions


def _load_user_events(db: Session, user_id: str) -> list[Event]:
    return list(
        db.scalars(select(Event).where(Event.user_id == user_id)).all()
    )


def _event_signature(db: Session, user_id: str) -> tuple[int, int]:
    """Event count and highest row id, recorded with the grouping metadata.

    They are stored for diagnostics and integrity checks; advancement does
    not query them, because every writer that mutates events also replaces
    the grouping in the same transaction.
    """
    count, max_id = db.execute(
        select(func.count(Event.id), func.coalesce(func.max(Event.id), 0)).where(
            Event.user_id == user_id
        )
    ).one()
    return int(count), int(max_id)


def _persist_grouping(
    db: Session, user_id: str, idle_gap_seconds: int, sessions: list[dict]
) -> SessionGrouping:
    """Replace the cached grouping of a user inside the current transaction."""
    event_count, max_event_id = _event_signature(db, user_id)

    db.query(GroupedSession).filter(GroupedSession.user_id == user_id).delete(
        synchronize_session=False
    )
    grouping = db.get(SessionGrouping, user_id)
    if grouping is None:
        grouping = SessionGrouping(user_id=user_id)
        db.add(grouping)
    grouping.idle_gap_seconds = idle_gap_seconds
    grouping.event_count = event_count
    grouping.max_event_id = max_event_id
    grouping.session_count = len(sessions)
    grouping.built_at = utc_now()
    db.flush()

    for seq, sess in enumerate(sessions):
        db.add(
            GroupedSession(
                user_id=user_id,
                seq=seq,
                session_key=sess["session_key"],
                start_time=sess["start"],
                end_time=sess["end"],
                event_ids=",".join(sess["event_ids"]),
            )
        )
    db.flush()
    return grouping


def _cached_sessions(db: Session, user_id: str) -> list[dict] | None:
    """Return the stored grouping as session dicts, or None if missing.

    No event scan is performed here: the signature stored with the grouping
    is trusted, because every code path that mutates events rebuilds it in
    the same transaction.
    """
    grouping = db.get(SessionGrouping, user_id)
    if grouping is None:
        return None
    rows = db.scalars(
        select(GroupedSession)
        .where(GroupedSession.user_id == user_id)
        .order_by(GroupedSession.seq)
    ).all()
    if len(rows) != grouping.session_count:
        return None
    return [
        {
            "session_key": row.session_key,
            "start": row.start_time,
            "end": row.end_time,
            "event_ids": row.event_ids.split(",") if row.event_ids else [],
        }
        for row in rows
    ]


def _grouping_for(
    db: Session, user_id: str, idle_gap_seconds: int
) -> tuple[list[dict], bool]:
    """Cached grouping for the current gap, rebuilding when needed.

    The cache is reused when it was built for the same idle interval.
    Old databases (no grouping yet) and gap changes rebuild it from the raw
    events on first use, in the caller's transaction.
    """
    grouping = db.get(SessionGrouping, user_id)
    if grouping is not None and grouping.idle_gap_seconds == idle_gap_seconds:
        cached = _cached_sessions(db, user_id)
        if cached is not None:
            return cached, False

    sessions = sessionize(_load_user_events(db, user_id), idle_gap_seconds)
    _persist_grouping(db, user_id, idle_gap_seconds, sessions)
    return sessions, True


def _maybe_build_grouping(
    db: Session, user_id: str, idle_gap_seconds: int
) -> list[dict]:
    """Cheap helper: only touch the cache if one exists or events are present."""
    grouping = db.get(SessionGrouping, user_id)
    if grouping is None:
        count, _ = _event_signature(db, user_id)
        if count == 0:
            return []
    sessions, _rebuilt = _grouping_for(db, user_id, idle_gap_seconds)
    return sessions


def current_sessions(
    db: Session, user_id: str, idle_gap_seconds: int, watermark: datetime
) -> list[dict]:
    """Sessions with their status under the current watermark.

    Served from the persisted grouping; the raw events are only read when
    the grouping must be built for the first time (old database or a new
    idle interval). May write the cache, so callers hold the user lock.
    """
    return classify_sessions(
        _maybe_build_grouping(db, user_id, idle_gap_seconds),
        watermark,
        idle_gap_seconds,
    )


def latest_revision(db: Session, user_id: str) -> Revision | None:
    return db.scalars(
        select(Revision)
        .where(Revision.user_id == user_id)
        .order_by(Revision.revision_no.desc())
        .limit(1)
    ).first()


def get_revision(db: Session, user_id: str, revision_no: int) -> Revision | None:
    return db.scalars(
        select(Revision).where(
            Revision.user_id == user_id, Revision.revision_no == revision_no
        )
    ).first()


def list_revisions(db: Session, user_id: str) -> list[Revision]:
    return list(
        db.scalars(
            select(Revision)
            .where(Revision.user_id == user_id)
            .order_by(Revision.revision_no.desc())
        ).all()
    )


def _published_map(snapshots) -> dict[str, SessionSnapshot]:
    return {s.session_key: s for s in snapshots if s.status == "published"}


def _diff_published(
    old_published: dict[str, SessionSnapshot], new_sessions: list[dict]
) -> tuple[list, list]:
    """Return (retractions, additions) against the previously published set."""
    new_published = {s["session_key"]: s for s in new_sessions if s["status"] == "published"}

    retractions = [
        old_published[key]
        for key in sorted(set(old_published) - set(new_published))
    ]
    additions = [
        new_published[key]
        for key in sorted(set(new_published) - set(old_published))
    ]
    return retractions, additions


def _persist_revision(
    db: Session,
    user_id: str,
    reason: str,
    detail: str,
    watermark: datetime,
    idle_gap_seconds: int,
    new_sessions: list[dict],
    retractions: list,
    additions: list,
) -> Revision:
    next_no = (
        db.scalar(
            select(func.coalesce(func.max(Revision.revision_no), 0)).where(
                Revision.user_id == user_id
            )
        )
        or 0
    ) + 1

    rev = Revision(
        user_id=user_id,
        revision_no=next_no,
        reason=reason,
        detail=detail,
        watermark=watermark,
        idle_gap_seconds=idle_gap_seconds,
    )
    db.add(rev)
    db.flush()

    for sess in new_sessions:
        db.add(
            SessionSnapshot(
                revision_id=rev.id,
                session_key=sess["session_key"],
                start_time=sess["start"],
                end_time=sess["end"],
                event_count=len(sess["event_ids"]),
                event_ids=",".join(sess["event_ids"]),
                status=sess["status"],
            )
        )

    for snap in retractions:
        db.add(
            RevisionChange(
                revision_id=rev.id,
                change_type="retract",
                session_key=snap.session_key,
                start_time=snap.start_time,
                end_time=snap.end_time,
                event_count=snap.event_count,
                event_ids=snap.event_ids,
            )
        )
    for sess in additions:
        db.add(
            RevisionChange(
                revision_id=rev.id,
                change_type="add",
                session_key=sess["session_key"],
                start_time=sess["start"],
                end_time=sess["end"],
                event_count=len(sess["event_ids"]),
                event_ids=",".join(sess["event_ids"]),
            )
        )

    db.flush()
    if FAIL_BEFORE_COMMIT:
        raise RuntimeError("simulated persistence failure (FAIL_BEFORE_COMMIT)")
    return rev


def _state_dicts(
    db: Session, user_id: str, idle_gap_seconds: int, watermark: datetime
) -> tuple[list, list, int | None]:
    """Current published/pending sessions and the latest revision number.

    When no revision exists yet (nothing was ever published/retracted), the
    state is classified from the cached grouping; the raw events are only
    read when the grouping must be built for the first time (old database or
    a new idle interval).
    """
    rev = latest_revision(db, user_id)
    if rev is not None:
        published = [
            _snap_to_dict(s) for s in rev.sessions if s.status == "published"
        ]
        pending = [
            _snap_to_dict(s) for s in rev.sessions if s.status == "pending"
        ]
        return published, pending, rev.revision_no

    sessions = classify_sessions(
        _maybe_build_grouping(db, user_id, idle_gap_seconds),
        watermark,
        idle_gap_seconds,
    )
    return (
        [s for s in sessions if s["status"] == "published"],
        [s for s in sessions if s["status"] == "pending"],
        None,
    )


def _snap_to_dict(snap: SessionSnapshot) -> dict:
    return {
        "session_key": snap.session_key,
        "start": snap.start_time,
        "end": snap.end_time,
        "event_ids": snap.event_ids.split(","),
        "status": snap.status,
    }


def import_events(
    db: Session, user_id: str, events: list[dict]
) -> dict:
    """Idempotent batch import; corrective revision on late-event impact.

    ``events`` is a list of ``{"event_id", "event_time"}`` dicts whose
    times are timezone-aware datetimes (normalized to UTC here). A revision
    is only created when the *published* set changes; the first import while
    everything is still pending creates no revision.
    """
    # In-request duplicate detection.
    seen: set[str] = set()
    duplicates_in_batch: set[str] = set()
    for ev in events:
        if ev["event_id"] in seen:
            duplicates_in_batch.add(ev["event_id"])
        seen.add(ev["event_id"])
    if duplicates_in_batch:
        raise ApiError(
            422,
            "duplicate event_id within import batch: "
            + ", ".join(sorted(duplicates_in_batch)),
        )

    cfg = get_or_create_user(db, user_id)
    existing_rows = db.scalars(
        select(Event).where(
            Event.user_id == user_id,
            Event.event_id.in_([e["event_id"] for e in events]),
        )
    ).all()
    existing = {row.event_id: row for row in existing_rows}

    conflicts = [
        eid
        for eid, row in existing.items()
        if any(
            e["event_id"] == eid
            and to_utc(e["event_time"]) != row.event_time
            for e in events
        )
    ]
    if conflicts:
        raise ApiError(
            409,
            "event_id already imported with a different event_time: "
            + ", ".join(sorted(conflicts)),
        )

    inserted = 0
    duplicate = 0
    for ev in events:
        eid = ev["event_id"]
        if eid in existing:
            duplicate += 1
            continue
        db.add(
            Event(
                user_id=user_id,
                event_id=eid,
                event_time=to_utc(ev["event_time"]),
            )
        )
        inserted += 1

    if inserted == 0:
        # Fully idempotent replay: nothing changes, no revision is created.
        # The stored grouping stays valid; raw events are not scanned again.
        db.flush()
        published, pending, _rev_no = _state_dicts(
            db, user_id, cfg.idle_gap_seconds, cfg.watermark
        )
        return {
            "summary": {
                "user_id": user_id,
                "received": len(events),
                "inserted": 0,
                "duplicate": duplicate,
                "conflicts": [],
            },
            "grouping_rebuilt": False,
            "new_revision_no": None,
            "retractions": [],
            "additions": [],
            "published_dicts": published,
            "pending_dicts": pending,
        }

    try:
        db.flush()
    except IntegrityError as exc:  # defensive: concurrent same-key import
        db.rollback()
        raise ApiError(409, "concurrent conflicting import") from exc

    # New events invalidate the cached grouping: rebuild it from the raw
    # events inside this same transaction. Besides feeding this response,
    # this guarantees the next watermark advance uses the post-import
    # grouping even when the import only moved pending sessions and created
    # no revision.
    new_sessions = classify_sessions(
        sessionize(_load_user_events(db, user_id), cfg.idle_gap_seconds),
        cfg.watermark,
        cfg.idle_gap_seconds,
    )
    _persist_grouping(db, user_id, cfg.idle_gap_seconds, new_sessions)

    prev = latest_revision(db, user_id)
    old_published = _published_map(prev.sessions) if prev else {}
    retractions, additions = _diff_published(old_published, new_sessions)

    new_rev_no: int | None = None
    if retractions or additions:
        detail = (
            f"import of {inserted} new event(s); "
            f"{len(retractions)} retracted, {len(additions)} newly published"
        )
        rev = _persist_revision(
            db,
            user_id,
            reason="late_import",
            detail=detail,
            watermark=cfg.watermark,
            idle_gap_seconds=cfg.idle_gap_seconds,
            new_sessions=new_sessions,
            retractions=retractions,
            additions=additions,
        )
        new_rev_no = rev.revision_no

    published = [s for s in new_sessions if s["status"] == "published"]
    pending = [s for s in new_sessions if s["status"] == "pending"]
    return {
        "summary": {
            "user_id": user_id,
            "received": len(events),
            "inserted": inserted,
            "duplicate": duplicate,
            "conflicts": [],
        },
        "grouping_rebuilt": True,
        "new_revision_no": new_rev_no,
        "retractions": retractions,
        "additions": additions,
        "published_dicts": published,
        "pending_dicts": pending,
    }


def advance_watermark(
    db: Session, user_id: str, new_watermark: datetime
) -> dict:
    """Move the watermark forward and publish newly eligible sessions.

    The watermark is monotonic: an equal or earlier value is rejected. No
    revision is created when the eligible published set does not change.
    """
    cfg = get_or_create_user(db, user_id)
    new_watermark = to_utc(new_watermark)
    if new_watermark < cfg.watermark:
        raise ApiError(
            409,
            "watermark can only move forward: "
            f"current={cfg.watermark.isoformat()} requested={new_watermark.isoformat()}",
        )
    if new_watermark == cfg.watermark:
        published, pending, rev_no = _state_dicts(
            db, user_id, cfg.idle_gap_seconds, cfg.watermark
        )
        return {
            "watermark": cfg.watermark,
            "new_revision_no": rev_no,
            "retractions": [],
            "additions": [],
            "published_dicts": published,
            "pending_dicts": pending,
        }

    cfg.watermark = new_watermark
    db.flush()

    # Pure watermark advancement: read only the stored grouping. Raw events
    # are touched solely when the grouping is absent (old database) or was
    # built for a different idle interval.
    new_sessions = classify_sessions(
        _grouping_for(db, user_id, cfg.idle_gap_seconds)[0],
        cfg.watermark,
        cfg.idle_gap_seconds,
    )

    prev = latest_revision(db, user_id)
    old_published = _published_map(prev.sessions) if prev else {}
    retractions, additions = _diff_published(old_published, new_sessions)

    new_rev_no: int | None = None
    if retractions or additions:
        detail = (
            f"watermark advanced to {new_watermark.isoformat()}; "
            f"{len(additions)} session(s) published"
        )
        rev = _persist_revision(
            db,
            user_id,
            reason="publish",
            detail=detail,
            watermark=cfg.watermark,
            idle_gap_seconds=cfg.idle_gap_seconds,
            new_sessions=new_sessions,
            retractions=retractions,
            additions=additions,
        )
        new_rev_no = rev.revision_no

    published = [s for s in new_sessions if s["status"] == "published"]
    pending = [s for s in new_sessions if s["status"] == "pending"]
    return {
        "watermark": cfg.watermark,
        "new_revision_no": new_rev_no,
        "retractions": retractions,
        "additions": additions,
        "published_dicts": published,
        "pending_dicts": pending,
    }
