"""Tests for the persisted session grouping cache.

Watermark advancement is expected to reuse a grouping that was computed for
the current idle gap instead of re-reading and re-sessionizing raw events.
The cache must behave exactly like a full recomputation, survive restarts, be
initialized lazily for old databases, be rebuilt on idle-gap changes and stay
consistent when imports / gap changes / watermark advances interleave.
"""
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, event as sa_event, func, select

from app import service
from app.models import Event, GroupedSession, Revision, SessionGrouping


def _iso(y, mo, d, h, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def import_events(client, uid, pairs):
    return client.post(
        "/api/events/import",
        json={
            "events": [
                {"user_id": uid, "event_id": eid, "event_time": ts}
                for eid, ts in pairs
            ]
        },
    )


def advance(client, uid, ts):
    return client.post(
        "/api/watermark/advance", json={"user_id": uid, "watermark": ts}
    )


def sessions_of(client, uid):
    return client.get(f"/api/users/{uid}/sessions").json()


def set_gap(client, uid, gap):
    return client.put(
        f"/api/users/{uid}/config", json={"idle_gap_seconds": gap}
    )


class EventReadSpy:
    """Counts raw-event loads performed through service._load_user_events."""

    def __init__(self, monkeypatch):
        self.calls = 0
        original = service._load_user_events

        def counting(db, user_id):
            self.calls += 1
            return original(db, user_id)

        monkeypatch.setattr(service, "_load_user_events", counting)

    def reset(self):
        self.calls = 0


def cached_state(session_factory, uid):
    """(gap, event_count, max_event_id, session_count, sessions) from the cache."""
    with session_factory() as db:
        grouping = db.get(SessionGrouping, uid)
        if grouping is None:
            return None
        rows = db.scalars(
            select(GroupedSession)
            .where(GroupedSession.user_id == uid)
            .order_by(GroupedSession.seq)
        ).all()
        return {
            "gap": grouping.idle_gap_seconds,
            "event_count": grouping.event_count,
            "max_event_id": grouping.max_event_id,
            "session_count": grouping.session_count,
            "sessions": [
                (r.session_key, r.start_time, r.end_time, r.event_ids)
                for r in rows
            ],
        }


def full_recompute(session_factory, uid):
    """Independent reference: sessions derived from every raw event."""
    with session_factory() as db:
        cfg = db.get(service.UserConfig, uid)
        events = db.scalars(select(Event).where(Event.user_id == uid)).all()
        sessions = service.classify_sessions(
            service.sessionize(events, cfg.idle_gap_seconds),
            cfg.watermark,
            cfg.idle_gap_seconds,
        )
        return [
            (
                s["session_key"],
                s["start"],
                s["end"],
                s["event_ids"],
                s["status"],
            )
            for s in sessions
        ]


def response_sessions(body):
    return [
        (s["session_key"], s["start_time"], s["end_time"], s["event_ids"], s["status"])
        for s in sorted(body["published"] + body["pending"], key=lambda s: s["start_time"])
    ]


def _match_full(session_factory, uid, resp_sessions):
    """Compare API sessions (formatted) with a full recomputation."""
    reference = full_recompute(session_factory, uid)
    session_key = [r[0] for r in reference]
    assert [s[0] for s in resp_sessions] == session_key
    assert [s[4] for s in resp_sessions] == [r[4] for r in reference]
    assert [s[3] for s in resp_sessions] == [r[3] for r in reference]


# ------------------------------------------------------------ cache creation
def test_import_creates_grouping_and_advance_does_not_read_events(
    client, session_factory, monkeypatch
):
    uid = "cache1"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
        ],
    )

    state = cached_state(session_factory, uid)
    assert state is not None
    assert state["gap"] == 600
    assert state["event_count"] == 3
    assert state["session_count"] == 2

    spy = EventReadSpy(monkeypatch)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert r.status_code == 200
    assert spy.calls == 0, "pure watermark advance must reuse the stored grouping"
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2"]
    _match_full(session_factory, uid, response_sessions(r.json()))

    spy.reset()
    r2 = advance(client, uid, _iso(2026, 11, 22, 10, 40))
    assert r2.status_code == 200
    assert spy.calls == 0
    assert r2.json()["new_revision_no"] == 2
    assert r2.json()["changes"][0]["event_ids"] == ["b1"]
    _match_full(session_factory, uid, response_sessions(r2.json()))


def test_advance_issues_no_queries_against_event_table(
    client, session_factory, engine
):
    """Hard proof at the SQL level: no SELECT touches the event table."""
    uid = "cache-sql"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
        ],
    )

    statements: list[str] = []

    def before_cursor_execute(conn, cursor, statement, parameters, context, many):
        statements.append(statement)

    sa_event.listen(engine, "before_cursor_execute", before_cursor_execute)
    try:
        r = advance(client, uid, _iso(2026, 11, 22, 10, 20))
    finally:
        sa_event.remove(engine, "before_cursor_execute", before_cursor_execute)

    assert r.status_code == 200
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2"]
    event_selects = [
        s for s in statements if "FROM event" in s.replace("\n", " ")
    ]
    assert event_selects == [], f"advance read raw events: {event_selects}"
    assert any("grouped_session" in s for s in statements)


def test_advance_after_restart_reuses_persisted_grouping(
    client, session_factory, monkeypatch
):
    uid = "cache-restart"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [("a1", _iso(2026, 11, 22, 10, 0)), ("a2", _iso(2026, 11, 22, 10, 8))],
    )
    before = cached_state(session_factory, uid)
    assert before is not None and before["session_count"] == 1

    # Simulate process restart: drop every in-process cache, keep the DB.
    service._user_locks.clear()

    spy = EventReadSpy(monkeypatch)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert r.status_code == 200
    assert spy.calls == 0, "grouping must survive a restart"
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2"]
    assert cached_state(session_factory, uid)["session_count"] == 1


# --------------------------------------------------------- late import effect
def test_import_changing_only_pending_still_refreshes_grouping(
    client, session_factory, monkeypatch
):
    """A late import may merge/reorder pending sessions without a revision.

    The next advance must still use the post-import grouping, even though no
    revision was generated by the import itself.
    """
    uid = "cache-pending"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
        ],
    )
    # Publish only the first session, second stays pending.
    r = advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2"]
    revs_before = len(client.get(f"/api/users/{uid}/revisions").json())

    # Late event 10:14 bridges a1/a2 with b1: that merges an already published
    # session, so it does create a retraction; then a second late event only
    # merges pending sessions (no published-set change) and must create no
    # revision while still updating the grouping.
    late = import_events(client, uid, [("c1", _iso(2026, 11, 22, 10, 30))])
    assert late.json()["new_revision_no"] is None
    assert len(client.get(f"/api/users/{uid}/revisions").json()) == revs_before
    after_import = cached_state(session_factory, uid)
    assert after_import is not None
    assert [s[3] for s in after_import["sessions"]][-1] == "b1,c1"

    spy = EventReadSpy(monkeypatch)
    r2 = advance(client, uid, _iso(2026, 11, 22, 10, 50))
    assert r2.status_code == 200
    assert spy.calls == 0
    body = r2.json()
    assert body["new_revision_no"] == revs_before + 1
    assert body["changes"][0]["event_ids"] == ["b1", "c1"]
    _match_full(session_factory, uid, response_sessions(body))


def test_pending_only_merge_after_late_import_uses_new_grouping(
    client, session_factory, monkeypatch
):
    """Regression: cached grouping must be replaced by a pending-only import."""
    uid = "cache-pending-merge"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
            ("c1", _iso(2026, 11, 22, 10, 40)),
        ],
    )
    # Watermark moved just past a1 only; b1 and c1 stay pending.
    advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert cached_state(session_factory, uid)["session_count"] == 3

    # Late event 10:30 bridges b1 and c1; still pending, no published change.
    r = import_events(client, uid, [("L", _iso(2026, 11, 22, 10, 30))])
    assert r.json()["new_revision_no"] is None
    cached = cached_state(session_factory, uid)
    assert [s[3] for s in cached["sessions"]] == ["a1", "b1,L,c1"]

    spy = EventReadSpy(monkeypatch)
    # Advance threshold to 10:40: only a1 published before; merged session end
    # is 10:40, so it is still pending -> no change. Then 11:00 publishes it.
    r2 = advance(client, uid, _iso(2026, 11, 22, 11, 0))
    assert r2.status_code == 200
    assert spy.calls == 0
    assert r2.json()["new_revision_no"] == 2
    assert r2.json()["changes"][0]["event_ids"] == ["b1", "L", "c1"]
    _match_full(session_factory, uid, response_sessions(r2.json()))


def test_bridge_import_then_republish_with_cache(client, session_factory, monkeypatch):
    """Late bridge retraction flow still works purely from the cache."""
    uid = "cache-bridge"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
            ("b2", _iso(2026, 11, 22, 10, 26)),
        ],
    )
    advance(client, uid, _iso(2026, 11, 22, 10, 36))
    late = import_events(client, uid, [("L1", _iso(2026, 11, 22, 10, 14))])
    assert late.json()["new_revision_no"] == 2
    assert [c["change_type"] for c in late.json()["changes"]] == ["retract"]

    spy = EventReadSpy(monkeypatch)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 40))
    assert r.status_code == 200
    assert spy.calls == 0
    assert r.json()["new_revision_no"] == 3
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2", "L1", "b1", "b2"]
    _match_full(session_factory, uid, response_sessions(r.json()))


# ------------------------------------------------------------ idle gap change
def test_idle_gap_change_rebuilds_grouping_on_next_use(
    client, session_factory, monkeypatch
):
    uid = "cache-gap"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("b1", _iso(2026, 11, 22, 10, 10)),  # 10m: separate at gap 600
            ("c1", _iso(2026, 11, 22, 10, 25)),  # 15m
        ],
    )
    assert cached_state(session_factory, uid)["session_count"] == 2

    set_gap(client, uid, 900)  # now all three merge into one session
    # The stored grouping is stale until the next use.
    assert cached_state(session_factory, uid)["gap"] == 600

    spy = EventReadSpy(monkeypatch)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 45))
    assert r.status_code == 200
    assert spy.calls == 1, "a gap change requires one rebuild from raw events"
    assert r.json()["changes"][0]["event_ids"] == ["a1", "b1", "c1"]
    state = cached_state(session_factory, uid)
    assert state["gap"] == 900
    assert state["session_count"] == 1

    spy.reset()
    r2 = advance(client, uid, _iso(2026, 11, 22, 11, 0))
    assert r2.status_code == 200
    assert spy.calls == 0, "subsequent advances reuse the rebuilt grouping"
    _match_full(session_factory, uid, response_sessions(r2.json()))


def test_gap_change_restores_previous_threshold_semantics(
    client, session_factory
):
    uid = "cache-gap2"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [("a1", _iso(2026, 11, 22, 10, 0)), ("a2", _iso(2026, 11, 22, 10, 5))],
    )
    advance(client, uid, _iso(2026, 11, 22, 10, 10))
    assert [c["change_type"] for c in advance(client, uid, _iso(2026, 11, 22, 10, 20)).json()["changes"]] == ["add"]

    # Gap change may unpublish the session (end 10:05 is no longer before
    # 10:20 - 3600). This must be a retraction computed from the rebuilt
    # grouping, identical to a full recomputation.
    set_gap(client, uid, 3600)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 20))  # equal watermark is a no-op
    assert r.json()["new_revision_no"] == 1
    r2 = advance(client, uid, _iso(2026, 11, 22, 10, 30))
    body = r2.json()
    assert [c["change_type"] for c in body["changes"]] == ["retract"]
    _match_full(session_factory, uid, response_sessions(body))


# -------------------------------------------------------------- old database
def test_old_database_without_grouping_is_initialized_lazily(
    client, session_factory, monkeypatch
):
    uid = "cache-old"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 20)),
        ],
    )
    # Emulate a database created by the previous version: no grouping rows.
    with session_factory() as db:
        db.execute(delete(GroupedSession))
        db.execute(delete(SessionGrouping))
        db.commit()
    assert cached_state(session_factory, uid) is None

    spy = EventReadSpy(monkeypatch)
    r = advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert r.status_code == 200
    assert spy.calls == 1, "first use must initialize the grouping"
    assert r.json()["new_revision_no"] == 1
    assert r.json()["changes"][0]["event_ids"] == ["a1", "a2"]

    state = cached_state(session_factory, uid)
    assert state is not None
    assert state["gap"] == 600
    assert state["event_count"] == 3
    assert state["session_count"] == 2

    # Later advances no longer scan events.
    spy.reset()
    advance(client, uid, _iso(2026, 11, 22, 10, 40))
    assert spy.calls == 0


# ------------------------------------------------------------ failure handling
def test_failed_import_keeps_previous_events_and_grouping(
    client, session_factory
):
    uid = "cache-fail-import"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [("a1", _iso(2026, 11, 22, 10, 0)), ("a2", _iso(2026, 11, 22, 10, 8))],
    )
    advance(client, uid, _iso(2026, 11, 22, 10, 20))
    before = cached_state(session_factory, uid)

    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = import_events(client, uid, [("L", _iso(2026, 11, 22, 10, 5))])
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    assert cached_state(session_factory, uid) == before
    assert [e["event_id"] for e in client.get(f"/api/users/{uid}/events").json()] == [
        "a1",
        "a2",
    ]
    assert len(client.get(f"/api/users/{uid}/revisions").json()) == 1


def test_failed_advance_keeps_previous_watermark_and_grouping(
    client, session_factory
):
    uid = "cache-fail-advance"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [
            ("a1", _iso(2026, 11, 22, 10, 0)),
            ("a2", _iso(2026, 11, 22, 10, 8)),
            ("b1", _iso(2026, 11, 22, 10, 25)),
        ],
    )
    advance(client, uid, _iso(2026, 11, 22, 10, 20))
    before = cached_state(session_factory, uid)
    revs_before = len(client.get(f"/api/users/{uid}/revisions").json())

    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = advance(client, uid, _iso(2026, 11, 22, 10, 40))
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    cfg = client.get(f"/api/users/{uid}/config").json()
    assert cfg["watermark"].startswith("2026-11-22T10:20:00")
    assert cached_state(session_factory, uid) == before
    assert len(client.get(f"/api/users/{uid}/revisions").json()) == revs_before

    ok = advance(client, uid, _iso(2026, 11, 22, 10, 40))
    assert ok.status_code == 200
    assert ok.json()["new_revision_no"] == revs_before + 1


def test_gap_change_then_failed_publish_keeps_old_gap_and_grouping(
    client, session_factory
):
    uid = "cache-fail-gap"
    set_gap(client, uid, 600)
    import_events(
        client,
        uid,
        [("a1", _iso(2026, 11, 22, 10, 0)), ("a2", _iso(2026, 11, 22, 10, 8))],
    )
    advance(client, uid, _iso(2026, 11, 22, 10, 20))
    before = cached_state(session_factory, uid)

    set_gap(client, uid, 300)
    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = advance(client, uid, _iso(2026, 11, 22, 10, 40))
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    assert cached_state(session_factory, uid) == before
    cfg = client.get(f"/api/users/{uid}/config").json()
    assert cfg["idle_gap_seconds"] == 300
    assert cfg["watermark"].startswith("2026-11-22T10:20:00")

    # Next advance rebuilds for gap 300 and matches a full recomputation.
    r = advance(client, uid, _iso(2026, 11, 22, 10, 40))
    assert r.status_code == 200
    assert cached_state(session_factory, uid)["gap"] == 300
    _match_full(session_factory, uid, response_sessions(r.json()))


# --------------------------------------------- interleaved replay consistency
@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_interleaved_operations_match_full_recompute(
    client, session_factory, seed
):
    """Random interleavings; after every step the cache equals a full recompute.

    Covers imports, idle-gap changes and watermark advances in submission
    order, including retractions/additions and pending-only imports.
    """
    import random

    rng = random.Random(seed)
    uid = f"interleave-{seed}"
    gap = 600
    set_gap(client, uid, gap)
    current = datetime(2026, 11, 22, 10, 0, tzinfo=timezone.utc)
    event_no = 0
    last_watermark = datetime(2026, 11, 22, 9, 50, tzinfo=timezone.utc)
    last_rev = None

    for _step in range(24):
        action = rng.choice(["import", "import", "advance", "gap"])
        if action == "import":
            batch = []
            for _ in range(rng.randint(1, 3)):
                event_no += 1
                # Mostly forward in time with occasional late events.
                if rng.random() < 0.35:
                    t = current - timedelta(minutes=rng.randint(1, 40))
                else:
                    t = current
                batch.append((f"e{event_no}", t.strftime("%Y-%m-%dT%H:%M:%SZ")))
                if t > current:
                    current = t
            r = import_events(client, uid, batch)
            assert r.status_code == 200, r.text
            body = r.json()
            if body["new_revision_no"] is not None:
                # Revisions are only produced when the published set changed.
                assert body["changes"]
                last_rev = body["new_revision_no"]
            else:
                assert not body["changes"]
            _match_full(session_factory, uid, response_sessions(body))
        elif action == "advance":
            last_watermark += timedelta(minutes=rng.randint(1, 15))
            r = advance(client, uid, last_watermark.strftime("%Y-%m-%dT%H:%M:%SZ"))
            assert r.status_code == 200, r.text
            body = r.json()
            if body["changes"]:
                last_rev = body["new_revision_no"]
            else:
                # No published-set change in this response -> latest revision
                # stays untouched.
                revs = client.get(f"/api/users/{uid}/revisions").json()
                assert (revs[0]["revision_no"] if revs else None) == last_rev
            _match_full(session_factory, uid, response_sessions(body))
        else:
            gap = rng.choice([300, 600, 900, 1800])
            set_gap(client, uid, gap)
            # The next advance needs the grouping for the new gap, so it must
            # rebuild it and then behave like a full recomputation.
            last_watermark += timedelta(minutes=rng.randint(1, 15))
            r = advance(client, uid, last_watermark.strftime("%Y-%m-%dT%H:%M:%SZ"))
            assert r.status_code == 200
            assert cached_state(session_factory, uid)["gap"] == gap
            body = r.json()
            if body["changes"]:
                last_rev = body["new_revision_no"]
            _match_full(session_factory, uid, response_sessions(body))

        # The stored grouping must always equal a sessionization of the raw
        # events at the gap it was built for (a full recomputation).
        state = cached_state(session_factory, uid)
        if state is not None:
            with session_factory() as db:
                rows = db.scalars(
                    select(GroupedSession)
                    .where(GroupedSession.user_id == uid)
                    .order_by(GroupedSession.seq)
                ).all()
                reference = service.sessionize(
                    db.scalars(select(Event).where(Event.user_id == uid)).all(),
                    state["gap"],
                )
            assert [
                (row.session_key, row.start_time, row.end_time, row.event_ids)
                for row in rows
            ] == [
                (s["session_key"], s["start"], s["end"], ",".join(s["event_ids"]))
                for s in reference
            ]


def test_import_does_not_create_revision_when_published_set_unchanged(
    client, session_factory
):
    uid = "cache-no-rev"
    set_gap(client, uid, 600)
    import_events(client, uid, [("a1", _iso(2026, 11, 22, 10, 0))])
    advance(client, uid, _iso(2026, 11, 22, 10, 20))
    assert len(client.get(f"/api/users/{uid}/revisions").json()) == 1

    # A far-future event is pending; the published set does not change.
    r = import_events(client, uid, [("z9", _iso(2026, 11, 22, 12, 0))])
    assert r.json()["new_revision_no"] is None
    assert len(client.get(f"/api/users/{uid}/revisions").json()) == 1
    # ... but the grouping now contains it for the next advance.
    assert cached_state(session_factory, uid)["session_count"] == 2
    with session_factory() as db:
        assert db.scalar(select(func.count(Revision.id)).where(Revision.user_id == uid)) == 1
