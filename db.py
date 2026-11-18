import sqlite3
import threading
import time

import config as C

LOCK = threading.RLock()
CONN: sqlite3.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  uid INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
  first_seen INTEGER, last_seen INTEGER, msgs INTEGER DEFAULT 0,
  saves INTEGER DEFAULT 0, status TEXT DEFAULT 'new',
  followed_back INTEGER DEFAULT 0, blocked INTEGER DEFAULT 0,
  day TEXT, used INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, thread_id TEXT,
  kind TEXT, detail TEXT, ts INTEGER);
CREATE TABLE IF NOT EXISTS media(
  id INTEGER PRIMARY KEY AUTOINCREMENT, uid INTEGER, thread_id TEXT,
  item_id TEXT, kind TEXT, file_id TEXT, tg_msg INTEGER, ts INTEGER);
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
"""


def init():
    global CONN
    C.DATA_DIR.mkdir(parents=True, exist_ok=True)
    CONN = sqlite3.connect(C.DB_FILE, check_same_thread=False)
    CONN.row_factory = sqlite3.Row
    with LOCK:
        CONN.executescript(SCHEMA)
        CONN.commit()


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


def now():
    return int(time.time())


def upsert_user(uid, username=None, full_name=None):
    row = one("SELECT uid FROM users WHERE uid=?", (uid,))
    if row:
        q("UPDATE users SET last_seen=?, username=COALESCE(?,username), "
          "full_name=COALESCE(?,full_name) WHERE uid=?",
          (now(), username, full_name, uid))
    else:
        q("INSERT INTO users(uid,username,full_name,first_seen,last_seen,status) "
          "VALUES(?,?,?,?,?, 'new')", (uid, username, full_name, now(), now()))


def get_user(uid):
    return one("SELECT * FROM users WHERE uid=?", (uid,))


def by_username(name):
    return one("SELECT * FROM users WHERE lower(username)=?", (name.lstrip("@").lower(),))


def set_status(uid, status):
    q("UPDATE users SET status=? WHERE uid=?", (status, uid))


def bump(uid, field):
    q(f"UPDATE users SET {field}={field}+1 WHERE uid=?", (uid,))


def event(uid, thread_id, kind, detail=""):
    q("INSERT INTO events(uid,thread_id,kind,detail,ts) VALUES(?,?,?,?,?)",
      (uid, str(thread_id or ""), kind, detail[:400], now()))


def events(limit=12):
    return many("SELECT e.*, u.username FROM events e LEFT JOIN users u ON u.uid=e.uid "
                "ORDER BY e.id DESC LIMIT ?", (limit,))


def stats():
    r = one("SELECT COUNT(*) c, SUM(saves) s, SUM(msgs) m FROM users")
    mut = one("SELECT COUNT(*) c FROM users WHERE followed_back=1")
    day = one("SELECT COUNT(*) c FROM users WHERE last_seen>?", (now() - 86400,))
    return {"users": r["c"] or 0, "saves": r["s"] or 0, "dms": r["m"] or 0,
            "mutual": mut["c"] or 0, "active24": day["c"] or 0}


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


def consume(uid, cost=1):
    """Returns (allowed, remaining)."""
    today = time.strftime("%Y-%m-%d")
    u = get_user(uid)
    if not u:
        return False, 0
    if u["status"] == "premium":
        return True, -1
    if u["day"] != today:
        q("UPDATE users SET day=?, used=0 WHERE uid=?", (today, uid))
        used = 0
    else:
        used = u["used"] or 0
    if used >= C.FREE_DAILY:
        return False, 0
    q("UPDATE users SET used=? WHERE uid=?", (used + cost, uid))
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
      "VALUES(?,?,?,?,?,?,?)", (uid, str(thread_id or ""), item_id, kind, file_id, tg_msg, now()))
