"""Media + DB persistence in Telegram. Host only ever holds temp files."""
import time

import config as C
import db
import tg


def snapshot_db():
    if not C.TG_DB_CHANNEL:
        return
    stamp = time.strftime("%Y%m%d-%H%M%S")
    path = C.TMP_DIR / f"db-{stamp}.sqlite"
    C.TMP_DIR.mkdir(parents=True, exist_ok=True)
    with open(C.DB_FILE, "rb") as src, open(path, "wb") as dst:
        dst.write(src.read())
    res = tg.send_doc(C.TG_DB_CHANNEL, path, f"db {stamp}")
    prev = db.meta_get("pinned_db_msg")
    tg.pin(C.TG_DB_CHANNEL, res["message_id"])
    if prev:
        tg.unpin(C.TG_DB_CHANNEL, int(prev))
        tg.delete(C.TG_DB_CHANNEL, int(prev))
    db.meta_set("pinned_db_msg", res["message_id"])
    path.unlink(missing_ok=True)


def restore_db():
    if not C.TG_DB_CHANNEL or not C.TG_TOKEN:
        return False
    try:
        file_id, _ = tg.pinned_document(C.TG_DB_CHANNEL)
        if not file_id:
            return False
        C.DATA_DIR.mkdir(parents=True, exist_ok=True)
        tg.download(file_id, C.DB_FILE)
        return True
    except Exception as e:
        print("db restore failed:", e)
        return False


def put(uid, thread_id, item_id, kind, path):
    """Upload temp file to storage channel, return (file_id, tg_msg)."""
    cap = f"{kind} | {uid} | {thread_id} | {item_id}"
    res = tg.send_doc(C.TG_STORAGE_CHANNEL, path, cap)
    return res["document"]["file_id"], res["message_id"]


def fetch(media_row, ext):
    dest = C.TMP_DIR / f"{media_row['id']}_{int(time.time())}.{ext}"
    tg.download(media_row["file_id"], dest)
    return dest
