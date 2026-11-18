"""Instagram side. Same trigger/cache/resolve core as before, now TG-backed."""
import json
import threading
import time
import traceback
from pathlib import Path

import requests
from instagrapi import Client
from instagrapi.exceptions import ChallengeRequired, DirectMessageRequestsDisabled, LoginRequired

import config as C
import db
import store
import styletxt as S

MEDIA_KINDS = {"raven", "voice", "photo", "video", "gif", "sticker", "clip", "post", "story", "link", "text"}

_wake = threading.Event()
_stop = threading.Event()
FRIEND_CACHE: dict[int, tuple[float, bool]] = {}


def log(*a):
    print(time.strftime("[%H:%M:%S]"), *a, flush=True)


# ------------------------------ media extraction ---------------------------- #
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


def extract(msg):
    """Return (kind, ext, url) for anything saveable, else None."""
    if msg.visual_media:
        hit = best_url(msg.visual_media)
        if hit:
            return ("raven", *hit)

    it = (msg.item_type or "")

    if msg.media:
        if msg.media.audio_url:
            return "voice", "m4a", str(msg.media.audio_url)
        if msg.media.video_url:
            return "video", "mp4", str(msg.media.video_url)
        if msg.media.thumbnail_url:
            return "photo", "jpg", str(msg.media.thumbnail_url)

    if msg.animated_media:
        try:
            imgs = msg.animated_media.get("images", {})
            cand = (imgs.get("fixed_height") or imgs.get("original") or {})
            url = cand.get("mp4") or cand.get("url") or cand.get("webp")
            if url:
                return ("sticker" if it == "sticker" else "gif",
                        "mp4" if url.endswith(".mp4") else "webp", url)
        except Exception:
            pass

    if msg.clip and msg.clip.video_url:
        return "clip", "mp4", str(msg.clip.video_url)

    if msg.media_share:
        if msg.media_share.video_url:
            return "post", "mp4", str(msg.media_share.video_url)
        if msg.media_share.image_versions2 and msg.media_share.image_versions2.candidates:
            c = max(msg.media_share.image_versions2.candidates, key=lambda x: (x.width or 0) * (x.height or 0))
            return "post", "jpg", c.url

    if msg.story_share and isinstance(msg.story_share, dict):
        m = msg.story_share.get("media") or {}
        if isinstance(m, dict):
            if m.get("video_url"):
                return "story", "mp4", m["video_url"]
            cands = ((m.get("image_versions2") or {}).get("candidates") or [])
            if cands:
                return "story", "jpg", cands[0]["url"]

    if msg.xma_share and msg.xma_share.video_url:
        return "link", "url", msg.xma_share.video_url

    if (msg.text or "").strip():
        return "text", "txt", None

    return None


def extract_from_reply(reply):
    if reply is None:
        return None
    if reply.visual_media:
        hit = best_url(reply.visual_media)
        if hit:
            return ("raven", *hit)
    if reply.media:
        if reply.media.audio_url:
            return "voice", "m4a", str(reply.media.audio_url)
        if reply.media.video_url:
            return "video", "mp4", str(reply.media.video_url)
        if reply.media.thumbnail_url:
            return "photo", "jpg", str(reply.media.thumbnail_url)
    return None


def download(url, ext, tag):
    C.TMP_DIR.mkdir(parents=True, exist_ok=True)
    path = C.TMP_DIR / f"{tag}.{ext}"
    with requests.get(url, headers={"User-Agent": C.UA}, stream=True, timeout=60) as r:
        r.raise_for_status()
        size = 0
        with open(path, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                size += len(chunk)
                if size > C.MAX_BYTES:
                    raise ValueError(f"over {C.MAX_BYTES // 1048576} MB")
                fh.write(chunk)
    if path.stat().st_size < 512:
        raise ValueError("truncated")
    return path


# ------------------------------ friendship gate ----------------------------- #
def is_admin(uid, thread=None):
    if int(uid) in C.BOT_ADMIN_UIDS:
        return True
    if thread is not None and str(uid) in {str(x) for x in (thread.admin_user_ids or [])}:
        return True
    return False


def mutual(cl, uid):
    cached = FRIEND_CACHE.get(uid)
    if cached and time.time() - cached[0] < 90:
        return cached[1]
    ok = False
    try:
        rel = cl.user_friendship(uid)
        ok = bool(rel.following and rel.followed_by)
        if rel.followed_by and not rel.following:
            cl.user_follow(uid)
            db.q("UPDATE users SET followed_back=1 WHERE uid=?", (uid,))
            time.sleep(0.8)
            ok = True
        if ok and db.get_user(uid) and db.get_user(uid)["status"] == "new":
            db.set_status(uid, "mutual")
    except Exception as e:
        log("friendship check failed:", e)
    FRIEND_CACHE[uid] = (time.time(), ok)
    return ok


def follow_back_loop(cl):
    while not _stop.is_set():
        try:
            me = str(cl.user_id)
            known = {r["uid"] for r in db.many("SELECT uid FROM users")}
            for u in cl.user_followers(me, amount=60):
                uid = int(u.pk)
                if uid == int(me):
                    continue
                db.upsert_user(uid, u.username, u.full_name)
                if uid in known and db.get_user(uid)["followed_back"]:
                    continue
                try:
                    cl.user_follow(uid)
                    db.q("UPDATE users SET followed_back=1 WHERE uid=?", (uid,))
                    db.event(uid, None, "followback", "auto followed back")
                    db.set_status(uid, "mutual")
                    log("followed back", u.username)
                except Exception as e:
                    log("follow-back failed:", e)
                time.sleep(1.2)
        except Exception as e:
            log("follow scan:", e)
        _stop.wait(120)


# ------------------------------ sending ------------------------------------- #
def reply_thread(cl, thread, msg, text):
    if C.DRY_RUN:
        return
    try:
        cl.direct_send(text, thread_ids=[thread.id], reply_to_message=msg)
    except Exception:
        try:
            cl.direct_send(text, thread_ids=[thread.id])
        except Exception:
            pass


def dm_user(cl, uid, text):
    if C.DRY_RUN:
        return
    cl.direct_send(text, user_ids=[uid])


def deliver(cl, thread, msg, kind, path):
    uid = int(msg.user_id)
    targets = {"thread_ids": [thread.id]}
    if thread.is_group:
        try:
            cl.direct_send(S.sc("saved ❕"), user_ids=[uid])
            time.sleep(1)
            targets = {"user_ids": [uid]}
        except DirectMessageRequestsDisabled:
            targets = {"thread_ids": [thread.id]}

    if kind == "text":
        return

    if kind == "voice":
        cl.direct_send_voice(path, **targets)
    elif kind in ("video", "clip", "gif") and path.suffix in (".mp4", ".mov"):
        cl.direct_send_video(path, **targets)
    else:
        cl.direct_send_photo(path, **targets)


# ------------------------------ handler ------------------------------------- #
def handle_save(cl, thread, msg, cache):
    uid = int(msg.user_id)
    user = db.get_user(uid)
    if user and user["blocked"]:
        reply_thread(cl, thread, msg, S.block("blocked", "ᴀᴄᴄᴇꜱꜱ ɪꜱ ᴅɪꜱᴀʙʟᴇᴅ ꜰᴏʀ ʏᴏᴜ"))
        return

    if not mutual(cl, uid):
        db.set_status(uid, "following")
        reply_thread(cl, thread, msg,
                     S.block("follow required",
                             "ꜰᴏʟʟᴏᴡ ᴛʜᴇ ʙᴏᴛ ᴀɴᴅ ɪᴛ ꜰᴏʟʟᴏᴡꜱ ʙᴀᴄᴋ ᴀᴜᴛᴏᴍᴀᴛɪᴄᴀʟʟʏ.\nᴛʜᴇɴ ꜱᴇɴᴅ ᴛʜᴇ ᴄᴏᴍᴍᴀɴᴅ ᴀɢᴀɪɴ."))
        return

    target_id = msg.reply.id if msg.reply else None
    hit = extract_from_reply(msg.reply)
    if not hit and target_id:
        hit = cache.get(target_id)
        if not hit:
            try:
                for m in cl.direct_messages(thread.id, amount=40):
                    got = extract(m)
                    if got:
                        cache[m.id] = got
                hit = cache.get(target_id)
            except Exception as e:
                log("refetch:", e)
    if not hit and target_id:
        try:
            orig = cl.direct_message(thread.id, target_id)
            hit = extract(orig)
        except Exception:
            pass

    if not hit:
        reply_thread(cl, thread, msg, S.block("nothing found", "ᴛʜᴇ ʀᴇᴘʟɪᴇᴅ ᴍᴇᴅɪᴀ ᴇxᴘɪʀᴇᴅ."))
        return

    kind, ext, url = hit

    if kind in ("raven", "voice"):
        ok, left = db.consume(uid)
        if not ok:
            db.set_status(uid, "limited")
            reply_thread(cl, thread, msg, S.block(
                "daily limit", f"ꜰʀᴇᴇ ᴀᴄᴄᴇꜱꜱ: {C.FREE_DAILY}/ᴅᴀʏ\nᴘʀᴇᴍɪᴜᴍ ⇝ ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ 💐"))
            return

    tmp = None
    try:
        if kind == "text":
            text = (msg.reply.text if msg.reply else "") or ""
            db.bump(uid, "saves")
            db.event(uid, thread.id, "text", text[:120])
            dm_user(cl, uid, S.block("saved message", text or "—"))
            return

        if kind == "link":
            db.bump(uid, "saves")
            db.event(uid, thread.id, "link", url)
            dm_user(cl, uid, S.block("saved link", url))
            return

        tmp = download(url, ext, f"{uid}_{target_id or msg.id}")
        file_id, tg_msg = store.put(uid, thread.id, target_id or msg.id, kind, tmp)
        db.add_media(uid, thread.id, target_id or msg.id, kind, file_id, tg_msg)
        db.bump(uid, "saves")
        db.event(uid, thread.id, kind, f"stored #{tg_msg}")

        local = store.fetch(db.one("SELECT * FROM media WHERE tg_msg=?", (tg_msg,)), ext)
        deliver(cl, thread, msg, kind, local)
        local.unlink(missing_ok=True)
        log("delivered", kind, "to", uid)
    except Exception as e:
        log("save failed:", e)
        reply_thread(cl, thread, msg, S.block("failed", f"ᴇʀʀᴏʀ ⇝ {e}"))
    finally:
        if tmp:
            tmp.unlink(missing_ok=True)


# ------------------------------ welcome ------------------------------------- #
def welcome_check(cl, thread, seen_users):
    tpl = db.get_setting(f"welcome:{thread.id}")
    if not tpl or not thread.is_group:
        return
    prev = seen_users.get(thread.id)
    current = {int(u.pk) for u in thread.users}
    if prev is None:
        seen_users[thread.id] = current
        return
    for uid in current - prev:
        try:
            u = cl.user_info(uid)
            text = tpl.replace("{name}", u.full_name or u.username).replace("{group}", thread.thread_title or "")
            cl.direct_send(S.sc(text), thread_ids=[thread.id])
        except Exception as e:
            log("welcome:", e)
    seen_users[thread.id] = current


def cmd_wlcum(cl, thread, msg, arg):
    uid = int(msg.user_id)
    if not is_admin(uid, thread):
        reply_thread(cl, thread, msg, S.block("denied", "ᴀᴅᴍɪɴꜱ ᴏɴʟʏ."))
        return
    key = f"welcome:{thread.id}"
    if not arg:
        cur = db.get_setting(key)
        reply_thread(cl, thread, msg, S.block(
            "welcome message", (cur or "—") + "\n\nᴜꜱᴀɢᴇ ⇝ .ᴡʟᴄᴜᴍ <ᴛᴇxᴛ>  |  .ᴡʟᴄᴜᴍ ʀᴇꜱᴇᴛ"))
        return
    if arg.lower() == "reset":
        db.del_setting(key)
        reply_thread(cl, thread, msg, S.block("welcome", "ᴄʟᴇᴀʀᴇᴅ 🧹"))
        return
    db.set_setting(key, arg)
    reply_thread(cl, thread, msg, S.block("welcome set", arg.replace("{name}", "@user") + "\n\nᴘʟᴀᴄᴇʜᴏʟᴅᴇʀꜱ ⇝ {name} {group}"))


# ------------------------------ poll ---------------------------------------- #
def poll(cl, process=True, seen_users=None):
    seen_users = seen_users if seen_users is not None else {}

    try:
        pending = cl.direct_requests(amount=10)
    except Exception:
        pending = []
    for thread in pending:
        for m in thread.messages[-1:]:
            if str(m.user_id) == str(cl.user_id):
                continue
            uid = int(m.user_id)
            db.upsert_user(uid, None, None)
            db.event(uid, thread.id, "request", (m.text or "")[:100])
            try:
                cl.direct_request_approve(thread.id)
                db.event(uid, thread.id, "accepted", "request approved")
                if process:
                    cl.direct_send(S.block("request accepted", "ʏᴏᴜ ᴄᴀɴ ɴᴏᴡ ᴜꜱᴇ ᴛʜᴇ ʙᴏᴛ 💐"), thread_ids=[thread.id])
            except Exception as e:
                log("approve:", e)

    threads = cl.direct_threads(amount=20, thread_message_limit=25)
    for thread in threads:
        welcome_check(cl, thread, seen_users)

        cache = {}
        for m in thread.messages:
            got = extract(m)
            if got and got[0] in ("raven", "voice", "photo", "video", "gif", "sticker"):
                cache[m.id] = got

        for m in reversed(thread.messages):
            if db.was_seen(thread.id, m.id):
                continue
            db.mark_seen(thread.id, m.id)
            uid = int(m.user_id or 0)
            if uid:
                db.upsert_user(uid, None, None)
            if not process or str(m.user_id) == str(cl.user_id):
                continue

            text = (m.text or "").strip()
            low = text.lower()

            db.bump(uid, "msgs")
            db.event(uid, thread.id, "dm" if not thread.is_group else "group", text[:120])

            if low.startswith(".wlcum"):
                cmd_wlcum(cl, thread, m, text[6:].strip())
                continue

            if low in C.ALIASES:
                log("trigger", thread.id, uid)
                try:
                    handle_save(cl, thread, m, cache)
                except LoginRequired:
                    raise
                except Exception:
                    log(traceback.format_exc())
                time.sleep(1)
