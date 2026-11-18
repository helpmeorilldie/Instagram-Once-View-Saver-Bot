#!/usr/bin/env python3
"""
IG .eww snap saver — single-file build.

Env vars:
  IG_SESSIONID      (required) value of the `sessionid` cookie
  TRIGGER           (default ".eww")
  POLL_INTERVAL     seconds between inbox polls (default 1.5)
  ONLY_THREADS      comma separated thread ids to restrict to
  PROCESS_BACKLOG   "1" to handle old messages on startup
  DRY_RUN           "1" to download but not send
  MAX_MB            max file size to download (default 60)
  DATA_DIR          storage dir (default ./data)

  DAILY_LIMIT       base saves per user per day, resets 00:00 IST (default 10)
  FOLLOW_CACHE_TTL  seconds a follow verdict is trusted (default 20)
  FOLLOW_FAST       seconds between new-follower checks (default 6)
  FOLLOW_FULL       seconds between full follower reconciles (default 240)
  WORKERS           trigger worker threads (default 3)

  ADMINS            comma separated usernames with .admin access
  TG_BOT_TOKEN      telegram bot token (optional)
  TG_CHAT_ID        channel for DB + log backups
  TG_MEDIA_CHAT_ID  separate private channel for every saved file
  BACKUP_INTERVAL   seconds between DB backups (default 900)
"""

import json
import os
import queue
import signal
import sys
import threading
import time
import traceback
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from instagrapi import Client
from instagrapi.exceptions import (
    ChallengeRequired,
    DirectMessageRequestsDisabled,
    LoginRequired,
)
load_dotenv()
# --------------------------------------------------------------------------- #
# config
# --------------------------------------------------------------------------- #
SESSIONID = os.getenv("IG_SESSIONID", "").strip()
TRIGGER = os.getenv("TRIGGER", ".eww").strip().lower()
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1.5"))
PROCESS_BACKLOG = os.getenv("PROCESS_BACKLOG") == "1"
DRY_RUN = os.getenv("DRY_RUN") == "1"
MAX_BYTES = int(float(os.getenv("MAX_MB", "60")) * 1024 * 1024)
ONLY_THREADS = {
    t.strip()
    for t in os.getenv("ONLY_THREADS", "").split(",")
    if t.strip()
}
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "10"))
FOLLOW_CACHE_TTL = float(os.getenv("FOLLOW_CACHE_TTL", "20"))
FOLLOW_FAST = float(os.getenv("FOLLOW_FAST", "6"))
FOLLOW_FULL = float(os.getenv("FOLLOW_FULL", "240"))
WORKERS = int(os.getenv("WORKERS", "3"))

ADMINS_ENV = [
    a.strip().lstrip("@").lower()
    for a in os.getenv("ADMINS", "ammar._.azam,noxmirage_").split(",")
    if a.strip()
]

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TG_MEDIA_CHAT_ID = os.getenv("TG_MEDIA_CHAT_ID", "").strip() or TG_CHAT_ID
TG_ENABLED = bool(TG_BOT_TOKEN and TG_CHAT_ID)
BACKUP_INTERVAL = float(os.getenv("BACKUP_INTERVAL", "900"))

MEDIA_DIR = DATA_DIR / "media"
ARCHIVE_DIR = DATA_DIR / "archive"
STATE_FILE = DATA_DIR / "state.json"
SETTINGS_FILE = DATA_DIR / "settings.json"

MAX_SEEN_PER_THREAD = 400
UA = "Instagram 269.0.0.18.75 Android"
IST = ZoneInfo("Asia/Kolkata")

STATS_CMD = ".stats"
ABOUT_CMD = ".about"
ADMIN_CMD = ".admin"
HELP_CMD = ".help"
WELCOME_CMD = ".wlcum"
PENDING_TTL = 300

DEV_HANDLE = "@noxmirage_"
DEV_NAME = "❕Miranox"
CREDIT = "Bʏ ‣ @Noxmirage_ ~𝑯𝒂𝒛𝒆"

_wake = threading.Event()
_follow_wake = threading.Event()
_stop = threading.Event()
START = time.time()

# --------------------------------------------------------------------------- #
# font helpers
# --------------------------------------------------------------------------- #
_SC = {
    "a": "ᴀ", "b": "ʙ", "c": "ᴄ", "d": "ᴅ", "e": "ᴇ", "f": "ꜰ", "g": "ɢ",
    "h": "ʜ", "i": "ɪ", "j": "ᴊ", "k": "ᴋ", "l": "ʟ", "m": "ᴍ", "n": "ɴ",
    "o": "ᴏ", "p": "ᴘ", "q": "ǫ", "r": "ʀ", "s": "ꜱ", "t": "ᴛ", "u": "ᴜ",
    "v": "ᴠ", "w": "ᴡ", "x": "x", "y": "ʏ", "z": "ᴢ",
}


def sc(text):
    return "".join(_SC.get(ch.lower(), ch) for ch in str(text))


def styled(text):
    return " ".join(w if not w else w[0].upper() + sc(w[1:]) for w in str(text).split(" "))


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


def ist_str(ts):
    try:
        return datetime.fromtimestamp(float(ts), IST).strftime("%d %b %Y %H:%M IST")
    except Exception:
        return "unknown"


def dur(seconds):
    seconds = int(seconds)
    d, seconds = divmod(seconds, 86400)
    h, seconds = divmod(seconds, 3600)
    m, _ = divmod(seconds, 60)
    return " ".join(p for p in [f"{d}d" if d else "", f"{h}h" if h else "", f"{m}m"] if p) or "0m"


# --------------------------------------------------------------------------- #
# media
# --------------------------------------------------------------------------- #
def best_url(visual):
    if visual is None or getattr(visual, "media", None) is None:
        return None
    m = visual.media
    vids = m.video_versions or []
    if vids:
        v = max(vids, key=lambda x: (x.width or 0) * (x.height or 0))
        return "mp4", v.url
    cands = (m.image_versions2.candidates if m.image_versions2 else []) or []
    if cands:
        c = max(cands, key=lambda x: (x.width or 0) * (x.height or 0))
        return "jpg", c.url
    return None


def url_from_shared(reply):
    if reply is None:
        return None
    dm = getattr(reply, "media", None)
    if dm is not None:
        if dm.video_url:
            return "mp4", str(dm.video_url)
        if dm.thumbnail_url:
            return "jpg", str(dm.thumbnail_url)
    share = getattr(reply, "media_share", None)
    if share is not None:
        if share.video_url:
            return "mp4", str(share.video_url)
        if share.image_versions2 and share.image_versions2.candidates:
            c = max(share.image_versions2.candidates,
                    key=lambda x: (x.width or 0) * (x.height or 0))
            return "jpg", c.url
    return None


def resolve_reply_media(reply):
    if reply is None:
        return None
    return best_url(getattr(reply, "visual_media", None)) or url_from_shared(reply)


def download(url, ext, tag):
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    path = MEDIA_DIR / f"{tag}.{ext}"
    with requests.get(url, headers={"User-Agent": UA}, stream=True, timeout=60) as r:
        r.raise_for_status()
        size = 0
        with open(path, "wb") as fh:
            for chunk in r.iter_content(65536):
                size += len(chunk)
                if size > MAX_BYTES:
                    raise ValueError(f"file larger than {MAX_BYTES // 1048576} MB")
                fh.write(chunk)
    if path.stat().st_size < 1024:
        raise ValueError("truncated download")
    return path


# --------------------------------------------------------------------------- #
# state
# --------------------------------------------------------------------------- #
def _blank_state():
    return {
        "seen": {}, "window": {}, "known_followers": [], "users": {}, "threads": {},
        "stats": {}, "welcome": {}, "members": {}, "admins": [], "banned": [],
        "archive_total": 0, "created": time.time(),
    }


def load_state():
    if STATE_FILE.exists():
        try:
            data = json.loads(STATE_FILE.read_text())
            for k, v in _blank_state().items():
                data.setdefault(k, v)
            return data
        except Exception:
            log("state unreadable, starting fresh")
    return _blank_state()


STATE = load_state()
LOCK = threading.RLock()


def mark_seen(thread_id, msg_id):
    with LOCK:
        bucket = STATE["seen"].setdefault(str(thread_id), [])
        if msg_id not in bucket:
            bucket.append(msg_id)
        if len(bucket) > MAX_SEEN_PER_THREAD:
            del bucket[: len(bucket) - MAX_SEEN_PER_THREAD]


def was_seen(thread_id, msg_id):
    with LOCK:
        return msg_id in STATE["seen"].get(str(thread_id), [])


def save_state():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with LOCK:
        tmp.write_text(json.dumps(STATE, indent=1))
    tmp.replace(STATE_FILE)


def ist_today():
    return datetime.now(IST).strftime("%Y-%m-%d")


def user_rec(uid):
    with LOCK:
        return STATE["users"].setdefault(
            str(uid), {"first_seen": time.time(), "archived": 0, "saves": {}, "credits": 0})


def touch_user(uid, username=None, name=None):
    with LOCK:
        rec = user_rec(uid)
        if username:
            rec["username"] = username
        if name:
            rec["name"] = name
        rec["last_seen"] = time.time()


def usage_today(uid):
    saves = (STATE["users"].get(str(uid)) or {}).get("saves") or {}
    return int(saves.get("c", 0)) if saves.get("d") == ist_today() else 0


def base_limit(uid):
    rec = STATE["users"].get(str(uid)) or {}
    return int(rec["limit"]) if "limit" in rec else DAILY_LIMIT


def credits(uid):
    return int((STATE["users"].get(str(uid)) or {}).get("credits", 0))


def allowance(uid):
    return base_limit(uid) + credits(uid)


def bump_usage(uid):
    with LOCK:
        rec = user_rec(uid)
        saves = rec.get("saves") or {}
        if saves.get("d") != ist_today():
            saves = {"d": ist_today(), "c": 0}
        if int(saves.get("c", 0)) >= base_limit(uid) and int(rec.get("credits", 0)) > 0:
            rec["credits"] = int(rec.get("credits", 0)) - 1
        saves["c"] = int(saves.get("c", 0)) + 1
        rec["saves"] = saves


def stat_rec(thread_id, uid):
    with LOCK:
        t = STATE["stats"].setdefault(str(thread_id), {})
        return t.setdefault(str(uid), {"msgs": 0, "deleted": 0, "first_seen": time.time()})


def bump_stat(thread_id, uid, key, amount=1):
    with LOCK:
        rec = stat_rec(thread_id, uid)
        rec[key] = int(rec.get(key, 0)) + amount
        if key == "msgs":
            rec["last_msg"] = time.time()


def archive_record(uid, thread_id, msg_id, path):
    with LOCK:
        rec = user_rec(uid)
        rec["archived"] = int(rec.get("archived", 0)) + 1
        STATE["archive_total"] = int(STATE.get("archive_total", 0)) + 1
        try:
            ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
            (ARCHIVE_DIR / f"{int(time.time())}_{uid}_{path.name}").write_bytes(path.read_bytes())
        except Exception as e:
            log("archive copy failed:", e)


def is_banned(uid):
    return int(uid) in {int(x) for x in STATE.get("banned", [])}


def admin_ids():
    return {int(x) for x in STATE.get("admins", [])}


def is_admin(uid):
    return int(uid) in admin_ids()


# --------------------------------------------------------------------------- #
# telegram
# --------------------------------------------------------------------------- #
def tg(method, chat_id=None, files=None, **data):
    if not TG_ENABLED:
        return
    data.setdefault("chat_id", chat_id or TG_CHAT_ID)
    try:
        r = requests.post(f"https://api.telegram.org/bot{TG_BOT_TOKEN}/{method}",
                          data=data, files=files, timeout=180)
        if r.status_code >= 400:
            log("tg", method, r.status_code, r.text[:160])
    except Exception as e:
        log("tg error:", e)


def tg_note(text):
    if TG_ENABLED:
        tg("sendMessage", text=text[:3800], disable_web_page_preview="true")


def tg_backup_media(path, uid, thread_id, username, ext):
    if not TG_ENABLED or not path.exists():
        return
    caption = f"archive | uid {uid} (@{username or 'unknown'}) | thread {thread_id} | {ist_today()}"
    with open(path, "rb") as fh:
        tg("sendDocument", chat_id=TG_MEDIA_CHAT_ID,
           files={"document": (path.name, fh)}, caption=caption[:900])


def tg_backup_db():
    if not TG_ENABLED:
        return
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        snap = DATA_DIR / "db_backup.json"
        with LOCK:
            payload = {
                "generated": datetime.now(IST).isoformat(),
                "archive_total": STATE.get("archive_total", 0),
                "users": STATE.get("users", {}),
                "known_followers": STATE.get("known_followers", []),
                "admins": STATE.get("admins", []),
                "welcome": STATE.get("welcome", {}),
                "threads": STATE.get("threads", {}),
            }
        snap.write_text(json.dumps(payload, indent=1))
        with open(snap, "rb") as fh:
            tg("sendDocument", files={"document": ("db_backup.json", fh)},
               caption=f"db backup | users {len(payload['users'])} | "
                       f"followers {len(payload['known_followers'])} | "
                       f"archived {payload['archive_total']}")
    except Exception as e:
        log("db backup failed:", e)


# --------------------------------------------------------------------------- #
# client
# --------------------------------------------------------------------------- #
def build_client():
    cl = Client()
    cl.delay_range = [0.4, 1.0]
    if SETTINGS_FILE.exists():
        try:
            cl.load_settings(SETTINGS_FILE)
            cl.login_by_sessionid(SESSIONID)
            cl.get_timeline_feed()
            log("logged in with cached device settings")
            return cl
        except Exception as e:
            log("cached settings failed (%s), re-logging" % e)

    cl = Client()
    cl.delay_range = [0.4, 1.0]
    cl.login_by_sessionid(SESSIONID)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cl.dump_settings(SETTINGS_FILE)
    log("logged in as", cl.username, "(uid %s)" % cl.user_id)
    return cl


# --------------------------------------------------------------------------- #
# follow gate
# --------------------------------------------------------------------------- #
_follow_cache = {}
_follow_lock = threading.Lock()


def _friendship(cl, uid):
    for name in ("user_friendship_v1", "user_friendship"):
        fn = getattr(cl, name, None)
        if fn is None:
            continue
        try:
            fs = fn(uid)
            if hasattr(fs, "followed_by"):
                return {"followed_by": bool(fs.followed_by),
                        "following": bool(getattr(fs, "following", False))}
        except Exception as e:
            log("friendship via", name, "failed:", e)
    try:
        raw = cl.private_request(f"friendships/show/{uid}/")
        if isinstance(raw, dict):
            return {"followed_by": bool(raw.get("followed_by")),
                    "following": bool(raw.get("following"))}
    except Exception as e:
        log("friendship via private_request failed:", e)
    return None


def is_follower(cl, uid, force=False):
    uid = int(uid)
    now = time.time()
    with _follow_lock:
        hit = _follow_cache.get(uid)
        if hit and not force and now - hit[1] < FOLLOW_CACHE_TTL:
            return hit[0]

    fs = _friendship(cl, uid)
    if fs is not None:
        verdict = fs["followed_by"]
    else:
        with LOCK:
            known = {int(x) for x in STATE.get("known_followers", [])}
        verdict = uid in known if known else (hit[0] if hit else True)

    with _follow_lock:
        _follow_cache[uid] = (verdict, now)
    return verdict


def fetch_followers(cl, amount):
    """Safely fetch follower IDs from supported response formats."""

    for name in ("user_followers_v1", "user_followers"):
        fn = getattr(cl, name, None)

        if not callable(fn):
            continue

        try:
            data = fn(cl.user_id, amount=amount)

            if data is None:
                log("Follower API returned empty response:", name)
                continue

            # Handle dictionary responses
            if isinstance(data, dict):
                users = data.values()

            # Handle list responses
            elif isinstance(data, (list, tuple, set)):
                users = data

            else:
                log("Unexpected follower response:", type(data).__name__)
                continue

            follower_ids = set()

            for user in users:
                try:
                    if isinstance(user, dict):
                        uid = user.get("pk") or user.get("id")
                    else:
                        uid = getattr(user, "pk", None)
                        uid = uid or getattr(user, "id", None)

                    if uid is not None:
                        follower_ids.add(int(uid))

                except (TypeError, ValueError):
                    continue

            return follower_ids

        except Exception as e:
            error = str(e)

            if "429" in error or "too many requests" in error.lower():
                log("Instagram rate limit detected. Pausing follower checks.")
                time.sleep(300)
                return None

            log("Follower fetch failed:", name, error)

    return None


def follow_back_one(cl, uid):
    try:
        cl.user_follow(uid)
    except Exception as e:
        log("follow back failed for", uid, e)
        return False

    handle = str(uid)
    try:
        info = cl.user_info(uid)
        handle = f"@{info.username}"
        touch_user(uid, info.username, info.full_name)
    except Exception:
        pass

    with _follow_lock:
        _follow_cache[uid] = (True, time.time())
    log("followed back", handle)
    tg_note(f"new follower, followed back: {handle}")
    return True


def follow_back_loop(cl):
    """Tight newest-follower watch + slow full reconcile."""
    known = {int(x) for x in STATE.get("known_followers", [])}

    if not known:
        base = fetch_followers(cl, 0)
        if base:
            known = base
            with LOCK:
                STATE["known_followers"] = sorted(known)
            save_state()
            log("follower baseline:", len(known))

    last_full = time.time()

    while not _stop.is_set():
        full = time.time() - last_full >= FOLLOW_FULL
        live = fetch_followers(cl, 0 if full else 2)
        if full:
            last_full = time.time()

        if live is not None:
            fresh = live - known
            if fresh:
                log("new follower(s):", len(fresh))
                for uid in fresh:
                    follow_back_one(cl, uid)
                    time.sleep(0.6)
                known |= fresh
                with LOCK:
                    STATE["known_followers"] = sorted(known)
                save_state()
            elif full:
                known = live
                with LOCK:
                    STATE["known_followers"] = sorted(known)
                save_state()

        _follow_wake.wait(FOLLOW_FAST)
        _follow_wake.clear()


# --------------------------------------------------------------------------- #
# messaging
# --------------------------------------------------------------------------- #
def send_text(cl, thread, msg, text, reply=False):
    if DRY_RUN:
        log("DRY_RUN text:", text.splitlines()[0])
        return
    try:
        if thread.is_group:
            kw = {"reply_to_message": msg} if reply else {}
            cl.direct_send(text, thread_ids=[thread.id], **kw)
        else:
            cl.direct_send(text, user_ids=[int(msg.user_id)])
    except DirectMessageRequestsDisabled:
        pass
    except Exception as e:
        log("send_text failed:", e)


def follow_prompt(msg):
    u = getattr(msg, "user", None)
    name = (getattr(u, "full_name", None) or getattr(u, "username", None) or "there")
    return "\n".join([
        f"👋 {styled('hey')} {name}!",
        styled("follow the bot account first to use it"),
        f"‣ @{BOT_USERNAME}",
        styled("then send") + f" {TRIGGER} " + styled("again"),
        "", CREDIT,
    ])


def limit_text(uid):
    return "\n".join([
        "🚫 " + styled("daily limit reached"),
        "◌ " + styled("used") + f" ⇝ {usage_today(uid)}/{base_limit(uid)}",
        "◌ " + styled("credits left") + f" ⇝ {credits(uid)}",
        "◌ " + styled("reset") + " ⇝ 00:00 IST",
        "", CREDIT,
    ])


def dashboard(cl, thread, msg, uid):
    u = getattr(msg, "user", None)
    rec = STATE["users"].get(str(uid), {})
    used = usage_today(uid)
    base = base_limit(uid)
    cred = credits(uid)
    following = is_follower(cl, uid)

    lines = []
    if not following:
        lines += [styled("you are not following the bot"), f"‣ @{BOT_USERNAME}", ""]

    lines += [
        "🎗️ " + styled("user profile"),
        "◌ " + styled("username") + " ⇝ @" + (rec.get("username") or getattr(u, "username", "unknown")),
        "◌ " + styled("name") + " ⇝ " + (rec.get("name") or getattr(u, "full_name", None) or "-"),
        "◌ " + styled("saved today") + f" ⇝ {used}/{base}",
        "◌ " + styled("free left") + f" ⇝ {max(0, base - used)}",
        "◌ " + styled("credits") + f" ⇝ {cred}",
        "◌ " + styled("archived") + f" ⇝ {rec.get('archived', 0)}",
        "◌ " + styled("following bot") + " ⇝ " + ("✅" if following else "❌"),
        "◌ " + styled("reset") + " ⇝ 00:00 IST",
        "",
        styled("reply") + f" {TRIGGER} " + styled("to any view once snap"),
        styled("send") + f" {HELP_CMD} " + styled("for commands"),
        "", CREDIT,
    ]
    send_text(cl, thread, msg, "\n".join(lines))


def help_card(cl, thread, msg):
    send_text(cl, thread, msg, "\n".join([
        "📖 " + styled("commands"),
        f"{TRIGGER} ⇝ " + styled("reply to a view once snap to save it"),
        f"{STATS_CMD} ⇝ " + styled("your profile and quota"),
        f"{ABOUT_CMD} @" + styled("user") + " ⇝ " + styled("info on a gc member"),
        f"{ABOUT_CMD} @" + BOT_USERNAME + " ⇝ " + styled("about this bot"),
        f"{WELCOME_CMD} ⇝ " + styled("gc welcome, admins only"),
        "", CREDIT,
    ]))


# --------------------------------------------------------------------------- #
# .about
# --------------------------------------------------------------------------- #
def resolve_uid(cl, thread, msg, arg):
    arg = arg.strip().lstrip("@")
    people = {u.username.lower(): int(u.pk) for u in (thread.users or []) if u.username}
    if arg.lower() in people:
        return people[arg.lower()], arg
    if msg.reply is not None:
        ru = getattr(msg.reply, "user", None)
        if ru is not None:
            return int(ru.pk), getattr(ru, "username", "unknown")
    if arg:
        try:
            return int(cl.user_id_from_username(arg)), arg
        except Exception:
            return None, arg
    return None, arg


def about_card(cl, thread, msg, arg):
    if not thread.is_group:
        send_text(cl, thread, msg, styled("about only works in group chats"), reply=True)
        return

    uid, handle = resolve_uid(cl, thread, msg, arg)
    if uid is not None and uid == int(cl.user_id):
        return bot_card(cl, thread, msg)
    if uid is None:
        send_text(cl, thread, msg,
                  styled("couldn't resolve that user, try") + f" {ABOUT_CMD} @username", reply=True)
        return

    rec = STATE["users"].get(str(uid), {})
    st = STATE["stats"].get(str(thread.id), {}).get(str(uid), {})
    username = rec.get("username") or handle or str(uid)
    name = rec.get("name") or "-"

    last_online = "hidden"
    try:
        info = cl.user_info(uid)
        name = info.full_name or name
        la = getattr(info, "last_activity_at", None) or getattr(info, "last_seen", None)
        if isinstance(la, datetime):
            last_online = ist_str(la.timestamp())
    except Exception:
        pass

    following = is_follower(cl, uid)
    joined = st.get("first_seen") or rec.get("first_seen")

    send_text(cl, thread, msg, "\n".join([
        "🔎 " + styled("gc user info"),
        "◌ " + styled("username") + f" ⇝ @{username}",
        "◌ " + styled("name") + f" ⇝ {name}",
        "◌ " + styled("uid") + f" ⇝ {uid}",
        "",
        "◌ " + styled("messages here") + f" ⇝ {st.get('msgs', 0)}",
        "◌ " + styled("deleted here") + f" ⇝ {st.get('deleted', 0)}",
        "◌ " + styled("joined") + f" ⇝ {ist_str(joined) if joined else 'unknown'}",
        "◌ " + styled("last message") + f" ⇝ {ist_str(st['last_msg']) if st.get('last_msg') else 'none'}",
        "◌ " + styled("last online") + f" ⇝ {last_online}",
        "◌ " + styled("follows bot") + " ⇝ " + ("✅" if following else "❌"),
        "◌ " + styled("credits") + f" ⇝ {credits(uid)}",
        "◌ " + styled("archived") + f" ⇝ {rec.get('archived', 0)}",
        "",
        styled("counts are from the moment tracking started"),
        "", CREDIT,
    ]), reply=True)


def bot_card(cl, thread, msg):
    send_text(cl, thread, msg, "\n".join([
        "🤖 " + styled("about this bot"),
        "",
        styled("saves view once photos and videos from instagram group chats and dms before they expire"),
        styled("reply") + f" {TRIGGER} " + styled("to any view once snap and it lands in your dm"),
        "",
        "◌ " + styled("works for") + " ⇝ " + styled("followers only, follows back automatically"),
        "◌ " + styled("free saves") + f" ⇝ {DAILY_LIMIT} " + styled("per day"),
        "◌ " + styled("reset") + " ⇝ 00:00 IST",
        "◌ " + styled("extra saves") + " ⇝ " + styled("credits from admins"),
        "◌ " + styled("users") + f" ⇝ {len(STATE.get('users', {}))}",
        "◌ " + styled("uptime") + f" ⇝ {dur(time.time() - START)}",
        "",
        "🛠️ " + styled("developer"),
        "◌ " + styled("username") + f" ⇝ {DEV_HANDLE}",
        "◌ " + styled("name") + f" ⇝ {DEV_NAME}",
        "◌ " + styled("role") + " ⇝ " + styled("bot author and maintainer"),
        "",
        styled("builds and maintains this saver, handles hosting, fixes and support"),
        "",
        styled("dm") + f" {DEV_HANDLE} " + styled("for help"),
        "", CREDIT,
    ]), reply=True)


# --------------------------------------------------------------------------- #
# .admin
# --------------------------------------------------------------------------- #
def admin_panel(cl, thread, msg):
    send_text(cl, thread, msg, "\n".join([
        "🛡️ " + styled("admin panel"),
        "◌ " + styled("users") + f" ⇝ {len(STATE.get('users', {}))}",
        "◌ " + styled("threads") + f" ⇝ {len(STATE.get('threads', {}))}",
        "◌ " + styled("archived") + f" ⇝ {STATE.get('archive_total', 0)}",
        "◌ " + styled("followers") + f" ⇝ {len(STATE.get('known_followers', []))}",
        "◌ " + styled("admins") + f" ⇝ {len(admin_ids())}",
        "◌ " + styled("banned") + f" ⇝ {len(STATE.get('banned', []))}",
        "◌ " + styled("uptime") + f" ⇝ {dur(time.time() - START)}",
        "",
        "◌ " + styled("commands"),
        f"{ADMIN_CMD} " + styled("stats") + " ⇝ " + styled("usage summary"),
        f"{ADMIN_CMD} " + styled("users") + " ⇝ " + styled("top savers"),
        f"{ADMIN_CMD} " + styled("find") + " @" + styled("user") + " ⇝ " + styled("lookup"),
        f"{ADMIN_CMD} " + styled("limit") + " @" + styled("user") + " N ⇝ " + styled("cap"),
        f"{ADMIN_CMD} " + styled("addcredit") + " @" + styled("user") + " N ⇝ " + styled("grant saves"),
        f"{ADMIN_CMD} " + styled("subcredit") + " @" + styled("user") + " N ⇝ " + styled("remove saves"),
        f"{ADMIN_CMD} " + styled("reset") + " @" + styled("user") + " ⇝ " + styled("clear today"),
        f"{ADMIN_CMD} " + styled("ban") + " / " + styled("unban") + " @" + styled("user"),
        f"{ADMIN_CMD} " + styled("addadmin") + " / " + styled("deladmin") + " @" + styled("user"),
        f"{ADMIN_CMD} " + styled("broadcast") + " " + styled("text"),
        f"{ADMIN_CMD} " + styled("backup") + " ⇝ " + styled("push db now"),
        "", CREDIT,
    ]), reply=True)


def admin_cmd(cl, thread, msg, body):
    uid = int(msg.user_id)
    if not is_admin(uid):
        send_text(cl, thread, msg, styled("admins only"), reply=True)
        return

    parts = body.strip().split()
    if not parts:
        return admin_panel(cl, thread, msg)
    sub, rest = parts[0].lower(), parts[1:]

    if sub == "stats":
        users = STATE.get("users", {})
        active = sum(1 for r in users.values() if (r.get("saves") or {}).get("d") == ist_today()
                     and int((r.get("saves") or {}).get("c", 0)) > 0)
        saves = sum(int((r.get("saves") or {}).get("c", 0)) for r in users.values()
                    if (r.get("saves") or {}).get("d") == ist_today())
        return send_text(cl, thread, msg, "\n".join([
            "📊 " + styled("stats"),
            "◌ " + styled("users") + f" ⇝ {len(users)}",
            "◌ " + styled("active today") + f" ⇝ {active}",
            "◌ " + styled("saves today") + f" ⇝ {saves}",
            "◌ " + styled("credits outstanding") + f" ⇝ {sum(int(r.get('credits', 0)) for r in users.values())}",
            "◌ " + styled("archived total") + f" ⇝ {STATE.get('archive_total', 0)}",
            "◌ " + styled("known followers") + f" ⇝ {len(STATE.get('known_followers', []))}",
            "◌ " + styled("uptime") + f" ⇝ {dur(time.time() - START)}",
            "", CREDIT,
        ]), reply=True)

    if sub == "users":
        ranked = sorted(STATE.get("users", {}).items(),
                        key=lambda kv: int(kv[1].get("archived", 0)), reverse=True)[:10]
        lines = ["🏆 " + styled("top savers")]
        lines += [f"{i}. @{r.get('username', u)} ⇝ {r.get('archived', 0)}"
                  for i, (u, r) in enumerate(ranked, 1)] or [styled("nobody yet")]
        return send_text(cl, thread, msg, "\n".join(lines + ["", CREDIT]), reply=True)

    if sub in ("find", "limit", "reset", "ban", "unban", "addadmin", "deladmin",
               "addcredit", "subcredit"):
        if not rest:
            return send_text(cl, thread, msg, styled("need a username"), reply=True)
        target, _ = resolve_uid(cl, thread, msg, rest[0])
        if target is None:
            return send_text(cl, thread, msg, styled("user not found"), reply=True)
        rec = user_rec(target)
        tag = rec.get("username") or rest[0]

        if sub == "find":
            return send_text(cl, thread, msg, "\n".join([
                "🔎 " + styled("user record"),
                "◌ " + styled("uid") + f" ⇝ {target}",
                "◌ " + styled("username") + f" ⇝ @{tag}",
                "◌ " + styled("name") + f" ⇝ {rec.get('name', '-')}",
                "◌ " + styled("saved today") + f" ⇝ {usage_today(target)}/{base_limit(target)}",
                "◌ " + styled("credits") + f" ⇝ {credits(target)}",
                "◌ " + styled("archived") + f" ⇝ {rec.get('archived', 0)}",
                "◌ " + styled("first seen") + f" ⇝ {ist_str(rec.get('first_seen'))}",
                "◌ " + styled("admin") + " ⇝ " + ("✅" if is_admin(target) else "❌"),
                "◌ " + styled("banned") + " ⇝ " + ("✅" if is_banned(target) else "❌"),
                "", CREDIT,
            ]), reply=True)

        if sub == "limit":
            if len(rest) < 2 or not rest[1].isdigit():
                return send_text(cl, thread, msg, styled("give a number"), reply=True)
            rec["limit"] = int(rest[1])
            save_state()
            return send_text(cl, thread, msg,
                             styled("limit set to") + f" {rest[1]} " + styled("for") + f" @{tag}",
                             reply=True)

        if sub in ("addcredit", "subcredit"):
            if len(rest) < 2 or not rest[1].isdigit():
                return send_text(cl, thread, msg, styled("give a number"), reply=True)
            cur = int(rec.get("credits", 0))
            rec["credits"] = max(0, cur + int(rest[1])) if sub == "addcredit" \
                else max(0, cur - int(rest[1]))
            save_state()
            return send_text(cl, thread, msg, "\n".join([
                "🎟️ " + styled("credits updated"),
                "◌ " + styled("user") + f" ⇝ @{tag}",
                "◌ " + styled("credits") + f" ⇝ {rec['credits']}",
                "", CREDIT,
            ]), reply=True)

        if sub == "reset":
            rec["saves"] = {}
            save_state()
            return send_text(cl, thread, msg, styled("usage cleared for") + f" @{tag}", reply=True)

        if sub in ("ban", "unban"):
            banned = {int(x) for x in STATE.get("banned", [])}
            banned.add(target) if sub == "ban" else banned.discard(target)
            STATE["banned"] = sorted(banned)
            save_state()
            return send_text(cl, thread, msg, styled(sub + "ned") + f" @{tag}", reply=True)

        ids = admin_ids()
        ids.add(target) if sub == "addadmin" else ids.discard(target)
        STATE["admins"] = sorted(ids)
        save_state()
        return send_text(cl, thread, msg, styled("admin list updated for") + f" @{tag}", reply=True)

    if sub == "broadcast":
        text = body[len("broadcast"):].strip()
        if not text:
            return send_text(cl, thread, msg, styled("give text to broadcast"), reply=True)
        send_text(cl, thread, msg, styled("broadcasting..."), reply=True)
        JOB_Q.put((broadcast_job, (cl, text)))
        return

    if sub == "backup":
        save_state()
        tg_backup_db()
        return send_text(cl, thread, msg, styled("backup pushed"), reply=True)

    return admin_panel(cl, thread, msg)


def broadcast_job(cl, text):
    sent = failed = 0
    for uid in list(STATE.get("users", {}).keys()):
        try:
            cl.direct_send(text, user_ids=[int(uid)])
            sent += 1
        except Exception:
            failed += 1
        time.sleep(1.2)
    tg_note(f"broadcast done: sent {sent}, failed {failed}")


# --------------------------------------------------------------------------- #
# trigger
# --------------------------------------------------------------------------- #
def send_to(cl, thread, msg, path, ext):
    uid = int(msg.user_id)
    if DRY_RUN:
        log("DRY_RUN would send", path)
        return

    if thread.is_group:
        try:
            cl.direct_send(styled("saved your") + f" {TRIGGER}", user_ids=[uid])
            time.sleep(0.5)
        except DirectMessageRequestsDisabled:
            log("  user %s blocks DM requests, replying in thread" % uid)
            cl.direct_send("", thread_ids=[thread.id], reply_to_message=msg)
            if ext == "mp4":
                cl.direct_send_video(path, thread_ids=[thread.id])
            else:
                cl.direct_send_photo(path, thread_ids=[thread.id])
            return

    if ext == "mp4":
        cl.direct_send_video(path, user_ids=[uid])
    else:
        cl.direct_send_photo(path, user_ids=[uid])


def handle_trigger(cl, thread, msg, cache):
    uid = int(msg.user_id)
    u = getattr(msg, "user", None)
    touch_user(uid, getattr(u, "username", None), getattr(u, "full_name", None))

    if is_banned(uid):
        send_text(cl, thread, msg, styled("you are banned from using this bot"), reply=True)
        return

    # force a fresh verdict so a just-followed user is never turned away
    if not is_follower(cl, uid, force=True):
        log("  not a follower")
        _follow_wake.set()
        send_text(cl, thread, msg, follow_prompt(msg), reply=True)
        return

    if usage_today(uid) >= allowance(uid):
        log("  quota exhausted")
        send_text(cl, thread, msg, limit_text(uid), reply=True)
        return

    target_id = msg.reply.id if msg.reply else None
    hit = resolve_reply_media(msg.reply)

    if not hit and target_id:
        hit = cache.get(target_id)
        if not hit:
            try:
                for m in cl.direct_messages(thread.id, amount=40):
                    got = best_url(m.visual_media)
                    if got:
                        cache[m.id] = got
                hit = cache.get(target_id)
            except Exception as e:
                log("  refetch failed:", e)

    if not hit:
        log("  no media on the replied message (expired or not media)")
        send_text(cl, thread, msg, styled("couldn't fetch that one, it expired"), reply=True)
        return

    ext, url = hit
    tag = f"{thread.id}_{target_id or msg.id}_{uid}"
    try:
        path = download(url, ext, tag)
    except Exception as e:
        log("  download failed:", e)
        send_text(cl, thread, msg, styled("download failed") + f" ({e})", reply=True)
        return

    log("  saved", path.name, "-> DM", uid)
    send_to(cl, thread, msg, path, ext)

    bump_usage(uid)
    archive_record(uid, thread.id, target_id or msg.id, path)
    save_state()
    tg_backup_media(path, uid, thread.id, getattr(u, "username", None), ext)


# --------------------------------------------------------------------------- #
# welcome
# --------------------------------------------------------------------------- #
_pending_welcome = {}


def thread_admin(thread, uid):
    admins = getattr(thread, "admin_user_ids", None)
    return int(uid) in {int(a) for a in admins} if admins else True


def welcome_cmd(cl, thread, msg, body):
    uid = int(msg.user_id)
    if not thread.is_group:
        send_text(cl, thread, msg, styled("welcome messages only work in group chats"), reply=True)
        return
    if not thread_admin(thread, uid):
        send_text(cl, thread, msg, styled("admins only"), reply=True)
        return

    cur = STATE["welcome"].get(str(thread.id))
    body = body.strip()

    if body.lower() == "off":
        with LOCK:
            STATE["welcome"].pop(str(thread.id), None)
        save_state()
        return send_text(cl, thread, msg, styled("welcome message disabled"), reply=True)

    if body.lower() == "show":
        if not cur:
            send_text(cl, thread, msg, styled("no welcome message set"), reply=True)
        else:
            send_text(cl, thread, msg, styled("current welcome") + ":\n\n" + cur["text"], reply=True)
        return

    if body:
        with LOCK:
            STATE["welcome"][str(thread.id)] = {"text": body, "by": uid, "set_at": time.time()}
        save_state()
        return send_text(cl, thread, msg, styled("welcome message saved"), reply=True)

    _pending_welcome[str(thread.id)] = (uid, time.time())
    send_text(cl, thread, msg, "\n".join([
        "📝 " + styled("send the welcome message now"),
        "◌ " + styled("placeholders") + " ⇝ {name} {username}",
        "◌ " + styled("preview") + f" ⇝ {WELCOME_CMD} show",
        "◌ " + styled("disable") + f" ⇝ {WELCOME_CMD} off",
        "", CREDIT,
    ]), reply=True)


def handle_new_members(cl, thread):
    tid = str(thread.id)
    people = {int(u.pk): u for u in (thread.users or []) if int(u.pk) != int(cl.user_id)}
    current = set(people)
    prev = STATE["members"].get(tid)

    if prev is None:
        with LOCK:
            STATE["members"][tid] = sorted(current)
        return

    joined = current - set(prev)
    with LOCK:
        STATE["members"][tid] = sorted(current)
    if not joined:
        return

    tpl = STATE["welcome"].get(tid)
    if not tpl:
        return

    for uid in joined:
        u = people[uid]
        text = tpl["text"].replace("{name}", u.full_name or u.username or "there")
        text = text.replace("{username}", "@" + (u.username or str(uid)))
        if "{username}" not in tpl["text"] and u.username:
            text = f"@{u.username}\n\n{text}"
        try:
            cl.direct_send(text, thread_ids=[thread.id])
            log("welcomed", u.username, "in", tid)
            time.sleep(1)
        except Exception as e:
            log("welcome failed:", e)


# --------------------------------------------------------------------------- #
# poll
# --------------------------------------------------------------------------- #
JOB_Q = queue.Queue()


def worker():
    while not _stop.is_set():
        try:
            fn, args = JOB_Q.get(timeout=1)
        except queue.Empty:
            continue
        try:
            fn(*args)
        except LoginRequired:
            log("worker: session dead")
        except Exception:
            log("worker error:\n" + traceback.format_exc())
        finally:
            JOB_Q.task_done()


def track_window(thread):
    tid = str(thread.id)
    current = {m.id: int(m.user_id) for m in thread.messages}
    prev = STATE["window"].get(tid)
    if prev and len(current) >= 15:
        for mid, owner in prev.items():
            if mid not in current:
                bump_stat(tid, owner, "deleted")
    with LOCK:
        STATE["window"][tid] = current


def poll(cl, process=True):
    threads = cl.direct_threads(amount=12, thread_message_limit=20)
    for thread in threads:
        if ONLY_THREADS and str(thread.id) not in ONLY_THREADS:
            continue

        with LOCK:
            STATE["threads"][str(thread.id)] = {
                "name": getattr(thread, "thread_title", None) or str(thread.id),
                "group": thread.is_group,
                "last": time.time(),
            }

        if thread.is_group and process:
            try:
                handle_new_members(cl, thread)
            except Exception:
                log("member scan failed:\n" + traceback.format_exc())

        if process:
            try:
                track_window(thread)
            except Exception:
                pass

        cache = {}
        for m in thread.messages:
            hit = best_url(m.visual_media)
            if hit:
                cache[m.id] = hit

        for m in reversed(thread.messages):
            if was_seen(thread.id, m.id):
                continue
            mark_seen(thread.id, m.id)
            if not process:
                continue
            if str(m.user_id) == str(cl.user_id):
                continue

            text = (m.text or "").strip()
            low = text.lower()
            uid = int(m.user_id)
            bump_stat(thread.id, uid, "msgs")

            if low.startswith(ADMIN_CMD):
                admin_cmd(cl, thread, m, text[len(ADMIN_CMD):])
                continue
            if low.startswith(WELCOME_CMD):
                welcome_cmd(cl, thread, m, text[len(WELCOME_CMD):])
                continue
            if low.startswith(ABOUT_CMD):
                about_card(cl, thread, m, text[len(ABOUT_CMD):])
                continue
            if low.startswith(HELP_CMD):
                help_card(cl, thread, m)
                continue

            pend = _pending_welcome.get(str(thread.id))
            if pend and int(pend[0]) == uid and text and not low.startswith(TRIGGER) \
                    and time.time() - pend[1] < PENDING_TTL:
                _pending_welcome.pop(str(thread.id), None)
                with LOCK:
                    STATE["welcome"][str(thread.id)] = {
                        "text": text, "by": uid, "set_at": time.time()}
                save_state()
                send_text(cl, thread, m, styled("welcome message saved"), reply=True)
                continue

            if low == TRIGGER:
                log("trigger in thread", thread.id, "from", uid)
                JOB_Q.put((handle_trigger, (cl, thread, m, cache)))
                continue

            if not thread.is_group and text:
                u = getattr(m, "user", None)
                touch_user(uid, getattr(u, "username", None), getattr(u, "full_name", None))
                JOB_Q.put((dashboard, (cl, thread, m, uid)))


def realtime_thread(cl):
    try:
        cl.realtime_on("direct", lambda payload: _wake.set())
    except Exception as e:
        log("realtime direct hook failed:", e)

    try:
        cl.realtime_on("on_new_follower", lambda payload: _follow_wake.set())
        log("realtime follower hook on")
    except Exception:
        pass

    try:
        rt = cl.realtime_connect()
        rt.direct_subscribe()
        log("realtime subscribed")
        while not _stop.is_set():
            cl.realtime_read_once()
    except Exception as e:
        log("realtime disabled:", e)


def backup_loop():
    while not _stop.is_set():
        if _stop.wait(BACKUP_INTERVAL):
            break
        save_state()
        tg_backup_db()


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
BOT_USERNAME = "bot"


def main():
    global BOT_USERNAME
    if not SESSIONID:
        sys.exit("IG_SESSIONID is not set")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    cl = build_client()
    BOT_USERNAME = cl.username
    log("bot account:", BOT_USERNAME, "| base limit", DAILY_LIMIT, "/day IST")

    ids = admin_ids()
    for name in ADMINS_ENV:
        try:
            ids.add(int(cl.user_id_from_username(name)))
            log("admin:", name)
        except Exception as e:
            log("admin resolve failed", name, e)
    ids.add(int(cl.user_id))
    STATE["admins"] = sorted(ids)
    save_state()

    if TG_ENABLED:
        tg_note(f"bot online as @{BOT_USERNAME}")

    for _ in range(WORKERS):
        threading.Thread(target=worker, daemon=True).start()

    log("seeding existing messages...")
    try:
        poll(cl, process=PROCESS_BACKLOG)
    except Exception:
        log(traceback.format_exc())
    save_state()

    threading.Thread(target=follow_back_loop, args=(cl,), daemon=True).start()
    if TG_ENABLED:
        threading.Thread(target=backup_loop, daemon=True).start()
    if os.environ.get("REALTIME", "1") == "1":
        threading.Thread(target=realtime_thread, args=(cl,), daemon=True).start()

    last_save = last_keepalive = time.time()
    log("watching for", TRIGGER, "every", POLL_INTERVAL, "s")

    while not _stop.is_set():
        _wake.wait(POLL_INTERVAL)
        _wake.clear()
        try:
            poll(cl)
        except LoginRequired:
            log("session died, re-logging")
            try:
                cl = build_client()
            except Exception:
                log(traceback.format_exc())
                time.sleep(20)
        except ChallengeRequired as e:
            log("challenge required:", e)
            time.sleep(20)
        except Exception as e:
            log("poll error:", e)
            time.sleep(3)

        if time.time() - last_save > 5:
            save_state()
            last_save = time.time()
        if time.time() - last_keepalive > 600:
            try:
                cl.get_timeline_feed()
            except Exception:
                pass
            last_keepalive = time.time()

    save_state()
    log("stopped")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: _stop.set())
    signal.signal(signal.SIGTERM, lambda *_: _stop.set())
    main()
