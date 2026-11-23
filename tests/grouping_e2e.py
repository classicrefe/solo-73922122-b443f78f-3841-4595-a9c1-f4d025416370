"""E2E check for the grouping cache against a running server.

Usage: python tests/grouping_e2e.py <setup|post-restart|gap-change>

``setup`` imports events, publishes one session, then performs a late import
that only merges pending sessions (no revision). ``post-restart`` is run after
the server process has been restarted against the same SQLite file and
verifies the persisted grouping is reused. ``gap-change`` changes the idle
interval and checks the grouping is rebuilt.
"""
import json
import os
import sys
import urllib.request

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")
U = "e2e-grouping"


def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def imp(uid, events):
    return call("POST", "/api/events/import", {
        "events": [{"user_id": uid, "event_id": e, "event_time": t} for e, t in events]
    })


def wm(uid, t):
    return call("POST", "/api/watermark/advance", {"user_id": uid, "watermark": t})


def revisions(uid):
    return call("GET", f"/api/users/{uid}/revisions")[1]


def setup():
    call("PUT", f"/api/users/{U}/config", {"idle_gap_seconds": 600})
    s, r = imp(U, [
        ("p1", "2026-11-22T10:00:00Z"),
        ("p2", "2026-11-22T10:20:00Z"),
        ("p3", "2026-11-22T10:40:00Z"),
    ])
    assert s == 200 and r["new_revision_no"] is None
    assert r["grouping_rebuilt"] is True
    print("setup import p1..p3:", r["summary"], "grouping_rebuilt:", r["grouping_rebuilt"])

    s, r = wm(U, "2026-11-22T10:20:00Z")  # publish p1 only
    assert s == 200 and r["new_revision_no"] == 1, r
    assert [c["event_ids"] for c in r["changes"]] == [["p1"]]
    print("setup wm 10:20:", [(c["change_type"], c["event_ids"]) for c in r["changes"]])

    # Pending-only merge: L@10:30 joins p2 and p3, published set unchanged.
    s, r = imp(U, [("L", "2026-11-22T10:30:00Z")])
    assert s == 200 and r["new_revision_no"] is None, r
    assert r["grouping_rebuilt"] is True
    assert [p["event_ids"] for p in r["pending"]] == [["p2", "L", "p3"]], r["pending"]
    print("setup late L (no revision), pending:", [p["event_ids"] for p in r["pending"]])


def post_restart():
    # The process was restarted; only the SQLite file survived.
    s, cfg = call("GET", f"/api/users/{U}/config")
    assert s == 200 and cfg["idle_gap_seconds"] == 600
    assert cfg["watermark"].startswith("2026-11-22T10:20:00")
    assert [x["revision_no"] for x in revisions(U)] == [1]

    s, r = wm(U, "2026-11-22T11:00:00Z")
    assert s == 200 and r["new_revision_no"] == 2, r
    assert [c["event_ids"] for c in r["changes"]] == [["p2", "L", "p3"]], r["changes"]
    assert [x["event_ids"] for x in r["published"]] == [["p1"], ["p2", "L", "p3"]]
    print("post-restart wm 11:00:", [(c["change_type"], c["event_ids"]) for c in r["changes"]])

    revs = revisions(U)
    assert [x["revision_no"] for x in revs] == [2, 1]
    print("post-restart revisions:", [(x["revision_no"], x["reason"]) for x in revs])


def gap_change():
    # gap 600 -> 300: each event is its own session; the previously published
    # merged session no longer exists, so a retract + adds revision is written.
    call("PUT", f"/api/users/{U}/config", {"idle_gap_seconds": 300})
    s, r = wm(U, "2026-11-22T12:00:00Z")
    assert s == 200, r
    changes = [(c["change_type"], c["event_ids"]) for c in r["changes"]]
    assert changes == [
        ("retract", ["p2", "L", "p3"]),
        ("add", ["p2"]),
        ("add", ["p3"]),
        ("add", ["L"]),
    ], changes
    published = [x["event_ids"] for x in r["published"]]
    assert published == [["p1"], ["p2"], ["L"], ["p3"]], published
    assert r["pending"] == []
    print("gap-change wm 12:00:", changes)
    print("gap-change published:", published)

    # Another advance reuses the rebuilt grouping.
    s, r2 = wm(U, "2026-11-22T12:10:00Z")
    assert s == 200 and r2["new_revision_no"] is None and r2["changes"] == []
    print("gap-change next advance: no revision (reused grouping)")


PHASES = {"setup": setup, "post-restart": post_restart, "gap-change": gap_change}
if __name__ == "__main__":
    PHASES[sys.argv[1]]()
    print(f"PHASE {sys.argv[1]} PASSED")
