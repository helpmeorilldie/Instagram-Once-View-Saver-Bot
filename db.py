import sqlite3
import threading
import time
from contextlib import contextmanager

import config as C

LOCK = threading.RLock()
CONN: sqlite3.Connection | None = None

# Only these columns may be incremented through bump() (it builds SQL with an f-string).
_COUNTERS = {"msgs", "saves", "used"}
# Statuses that follow-state must never overwrite.
_PINNED_STATUS = ("blocked", "premium")

SCHEMA = """
CREATE TABLE IF NOT EXISTS seen(
  thread_id TEXT, msg_id TEXT, ts INTEGER, PRIMARY KEY(thread_id, msg_id));
CREATE TABLE IF NOT EXISTS users(
  uid INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
  first_seen INTEGER, last_seen INTEGER, msgs INTEGER DEFAULT 0,
  saves INTEGER DEFAULT 0, status TEXT DEFAULT 'new',
  followed_back INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0,
  day TEXT, used INTEGER DEFAULT 0,
  follows_me INTEGER DEFAULT 0, i_follow INTEGER DEFAULT 0, rel_checked INTEGER);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, thread_id TEXT,
  kind TEXT, detail TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS media(
  id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, thread_id TEXT,
  item_id TEXT, kind TEXT, file_id TEXT, tg_msg INTEGER, ts INTEGER);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""

INDEXES = """
CREATE INDEX IF NOT EXISTS idx_users_username ON users(lower(username));
CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen);
CREATE INDEX IF NOT EXISTS idx_events_uid ON events(uid);
CREATE INDEX IF NOT EXISTS idx_media_uid ON media(uid);
CREATE INDEX IF NOT EXISTS idx_seen_ts ON seen(ts);
"""


def now():
    return int(time.time())


# ───────────────────────── connection / migrations ─────────────────────────

def _columns(table):
    return {r["name"] for r in CONN.execute(f"PRAGMA table_info({table})")}


def _migrate():
    """Bring databases created by older versions up to date (idempotent)."""
    ucols = _columns("users")
    if "follows_me" not in ucols:
        CONN.execute("ALTER TABLE users ADD COLUMN follows_me INTEGER DEFAULT 0")
        CONN.execute("ALTER TABLE users ADD COLUMN i_follow INTEGER DEFAULT 0")
        CONN.execute("ALTER TABLE users ADD COLUMN rel_checked INTEGER")
        # old followed_back=1 meant "mutual", so both directions were true
        CONN.execute("UPDATE users SET follows_me=1, i_follow=1 WHERE followed_back=1")
    if "ts" not in _columns("seen"):
        CONN.execute("ALTER TABLE seen ADD COLUMN ts INTEGER")
        CONN.execute("UPDATE seen SET ts=?", (now(),))
    # keep the legacy `blocked` flag in step with status
    CONN.execute("UPDATE users SET blocked=1 WHERE status='blocked' AND blocked=0")


def init():
    global CONN
    C.DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONN = sqlite3.connect(C.DB_FILE, check_same_thread=False)
    CONN.row_factory = sqlite3.Row
    with LOCK:
        CONN.execute("PRAGMA journal_mode=WAL")
        CONN.execute("PRAGMA synchronous=NORMAL")
        CONN.execute("PRAGMA busy_timeout=5000")
        CONN.executescript(SCHEMA)
        _migrate()
        CONN.executescript(INDEXES)
        CONN.commit()


# ───────────────────────────── query helpers ─────────────────────────────

def q(sql, args=()):
    with LOCK:
        cur = CONN.execute(sql, args)
        CONN.commit()
        return cur


def one(sql, args=()):
    with LOCK:
        return CONN.execute(sql, args).fetchone()


def many(sql, args=()):
    with LOCK:
        return CONN.execute(sql, args).fetchall()


@contextmanager
def tx():
    """Atomic multi-statement block. Use the yielded connection directly
    (not q/one/many) so nothing commits half-way."""
    with LOCK:
        try:
            yield CONN
            CONN.commit()
        except Exception:
            CONN.rollback()
            raise


def backup_to(path):
    """Consistent copy of the live database (safe while the bot is running).
    Use this for the Telegram snapshot instead of copying bot.sqlite."""
    with LOCK:
        dst = sqlite3.connect(str(path))
        try:
            CONN.backup(dst)
        finally:
            dst.close()


# ───────────────────────────────── seen ─────────────────────────────────

def mark_seen(thread_id, msg_id):
    q("INSERT OR IGNORE INTO seen(thread_id,msg_id,ts) VALUES(?,?,?)",
      (str(thread_id), str(msg_id), now()))


def was_seen(thread_id, msg_id):
    return one("SELECT 1 FROM seen WHERE thread_id=? AND msg_id=?",
               (str(thread_id), str(msg_id))) is not None


def prune(seen_days=30, keep_events=5000):
    """Call occasionally (e.g. once a day) so the tables don't grow forever."""
    q("DELETE FROM seen WHERE ts < ?", (now() - seen_days * 86400,))
    q("DELETE FROM events WHERE id <= (SELECT MAX(id) FROM events) - ?", (keep_events,))


# ───────────────────────────────── users ─────────────────────────────────

def upsert_user(uid, username=None, full_name=None):
    uid = int(uid)
    t = now()
    q("INSERT INTO users(uid,username,full_name,first_seen,last_seen,status) "
      "VALUES(?,?,?,?,?,'new') "
      "ON CONFLICT(uid) DO UPDATE SET last_seen=excluded.last_seen, "
      "username=COALESCE(excluded.username, username), "
      "full_name=COALESCE(excluded.full_name, full_name)",
      (uid, username, full_name, t, t))


def get_user(uid):
    return one("SELECT * FROM users WHERE uid=?", (int(uid),))


def by_username(name):
    return one("SELECT * FROM users WHERE lower(username)=?", (name.lstrip("@").lower(),))


def set_status(uid, status):
    q("UPDATE users SET status=?, blocked=? WHERE uid=?",
      (status, int(status == "blocked"), int(uid)))


def bump(uid, field):
    if field not in _COUNTERS:
        raise ValueError(f"cannot bump {field!r}")
    q(f"UPDATE users SET {field}={field}+1 WHERE uid=?", (int(uid),))


# ─────────────────────── follow / follower relationship ───────────────────────
# Source of truth: follows_me (they follow me) + i_follow (I follow them).
# mutual == both. `followed_back` and `status` are derived from those two and
# kept in sync here, so the rest of the bot can keep reading them.

def _derived_status(current, mutual):
    if current in _PINNED_STATUS:
        return current          # blocked / premium are never overwritten
    return "mutual" if mutual else "new"


def set_relationship(uid, follows_me=None, i_follow=None):
    """Update one or both directions for a single user. Returns the new
    state as (follows_me, i_follow, mutual), or None if the user is unknown."""
    with tx() as c:
        r = c.execute("SELECT follows_me, i_follow, status FROM users WHERE uid=?",
                      (int(uid),)).fetchone()
        if not r:
            return None
        fm = int(bool(r["follows_me"] if follows_me is None else follows_me))
        fi = int(bool(r["i_follow"] if i_follow is None else i_follow))
        mutual = int(fm and fi)
        c.execute("UPDATE users SET follows_me=?, i_follow=?, followed_back=?, "
                  "status=?, rel_checked=? WHERE uid=?",
                  (fm, fi, mutual, _derived_status(r["status"], mutual), now(), int(uid)))
    return bool(fm), bool(fi), bool(mutual)


def sync_relationships(follower_ids, following_ids):
    """Reconcile every known user against full follower / following ID lists
    fetched from Instagram. Match on numeric user id, never on username.

    Pass COMPLETE lists only. If a fetch failed or came back partial, do not
    call this: it would mark everyone missing from the list as unfollowed.
    Returns a list of change dicts (for logging / notifying the admin).
    """
    followers = {int(x) for x in follower_ids}
    following = {int(x) for x in following_ids}
    ts = now()
    changes = []
    with tx() as c:
        for r in c.execute("SELECT uid, status, follows_me, i_follow FROM users").fetchall():
            fm = int(r["uid"] in followers)
            fi = int(r["uid"] in following)
            mutual = int(fm and fi)
            if fm != r["follows_me"] or fi != r["i_follow"]:
                changes.append({"uid": r["uid"],
                                "was_follows_me": bool(r["follows_me"]), "follows_me": bool(fm),
                                "was_i_follow": bool(r["i_follow"]), "i_follow": bool(fi),
                                "mutual": bool(mutual)})
            c.execute("UPDATE users SET follows_me=?, i_follow=?, followed_back=?, "
                      "status=?, rel_checked=? WHERE uid=?",
                      (fm, fi, mutual, _derived_status(r["status"], mutual), ts, r["uid"]))
    return changes


def is_mutual(uid):
    u = get_user(uid)
    return bool(u and u["follows_me"] and u["i_follow"])


def refresh_status(uid):
    """Recompute status from follow state (use after /unblock)."""
    u = get_user(uid)
    if not u:
        return None
    mutual = bool(u["follows_me"] and u["i_follow"])
    new = "mutual" if mutual else "new"
    set_status(uid, new)
    return new


# ───────────────────────────── events / stats ─────────────────────────────

def event(uid, thread_id, kind, detail=""):
    q("INSERT INTO events(uid,thread_id,kind,detail,ts) VALUES(?,?,?,?,?)",
      (uid, str(thread_id or ""), kind, (detail or "")[:400], now()))


def events(limit=12):
    return many("SELECT e.*, u.username FROM events e LEFT JOIN users u ON u.uid=e.uid "
                "ORDER BY e.id DESC LIMIT ?", (limit,))


def stats():
    r = one("SELECT COUNT(*) c, SUM(saves) s, SUM(msgs) m FROM users")
    mut = one("SELECT COUNT(*) c FROM users WHERE follows_me=1 AND i_follow=1")
    fol = one("SELECT COUNT(*) c FROM users WHERE follows_me=1")
    ing = one("SELECT COUNT(*) c FROM users WHERE i_follow=1")
    day = one("SELECT COUNT(*) c FROM users WHERE last_seen>?", (now() - 86400,))
    return {"users": r["c"] or 0, "saves": r["s"] or 0, "dms": r["m"] or 0,
            "mutual": mut["c"] or 0, "followers": fol["c"] or 0,
            "following": ing["c"] or 0, "active24": day["c"] or 0}


# ───────────────────────────── settings / meta ─────────────────────────────

def get_setting(key, default=None):
    r = one("SELECT value FROM settings WHERE key=?", (key,))
    return r["value"] if r else default


def set_setting(key, value):
    q("INSERT INTO settings(key,value) VALUES(?,?) "
      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))


def del_setting(key):
    q("DELETE FROM settings WHERE key=?", (key,))


def meta_set(key, value):
    q("INSERT INTO meta(key,value) VALUES(?,?) "
      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))


def meta_get(key):
    r = one("SELECT value FROM meta WHERE key=?", (key,))
    return r["value"] if r else None


# ───────────────────────────── daily quota ─────────────────────────────

def consume(uid, cost=1):
    """Returns (allowed, remaining). Atomic: two threads can't both spend the last credit."""
    today = time.strftime("%Y-%m-%d")
    with LOCK:
        u = get_user(uid)
        if not u:
            return False, 0
        if u["status"] == "premium":
            return True, -1
        used = (u["used"] or 0) if u["day"] == today else 0
        if used + cost > C.FREE_DAILY:
            return False, max(0, C.FREE_DAILY - used)
        q("UPDATE users SET day=?, used=? WHERE uid=?", (today, used + cost, int(uid)))
        return True, C.FREE_DAILY - used - cost


def remaining(uid):
    u = get_user(uid)
    if not u:
        return C.FREE_DAILY
    if u["status"] == "premium":
        return -1
    if u["day"] != time.strftime("%Y-%m-%d"):
        return C.FREE_DAILY
    return max(0, C.FREE_DAILY - (u["used"] or 0))


def add_media(uid, thread_id, item_id, kind, file_id, tg_msg):
    q("INSERT INTO media(uid,thread_id,item_id,kind,file_id,tg_msg,ts) "
      "VALUES(?,?,?,?,?,?,?)",
      (uid, str(thread_id or ""), item_id, kind, file_id, tg_msg, now()))
