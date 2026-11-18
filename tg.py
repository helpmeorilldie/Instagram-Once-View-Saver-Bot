"""Thin Telegram Bot API client: storage upload/download + admin control polling."""
import json
import threading
import time

import requests

import config as C

BASE = f"https://api.telegram.org/bot{C.TG_TOKEN}"
FILE_BASE = f"https://api.telegram.org/file/bot{C.TG_TOKEN}"


def call(method: str, timeout: int = 60, **payload):
    r = requests.post(f"{BASE}/{method}", data=payload, timeout=timeout)
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"telegram {method}: {data.get('description')}")
    return data["result"]


def send_text(chat, text, reply_to=None, markup=None):
    payload = {"chat_id": chat, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    if markup:
        payload["reply_markup"] = json.dumps(markup)
    return call("sendMessage", **payload)


def send_doc(chat, path, caption=""):
    with open(path, "rb") as fh:
        r = requests.post(
            f"{BASE}/sendDocument",
            data={"chat_id": chat, "caption": caption[:1000]},
            files={"document": (path.name, fh)},
            timeout=300,
        )
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"telegram sendDocument: {data.get('description')}")
    return data["result"]


def pin(chat, message_id, notify=False):
    return call("pinChatMessage", chat_id=chat, message_id=message_id,
                disable_notification=str(not notify).lower())


def unpin(chat, message_id):
    try:
        call("unpinChatMessage", chat_id=chat, message_id=message_id)
    except Exception:
        pass


def delete(chat, message_id):
    try:
        call("deleteMessage", chat_id=chat, message_id=message_id)
    except Exception:
        pass


def download(file_id, dest):
    path = call("getFile", file_id=file_id)["file_path"]
    with requests.get(f"{FILE_BASE}/{path}", stream=True, timeout=300) as r:
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                fh.write(chunk)
    return dest


def pinned_document(chat):
    """Latest pinned doc in the DB channel, if any."""
    info = call("getChat", chat_id=chat)
    pinned = info.get("pinned_message") or {}
    doc = pinned.get("document")
    if doc:
        return doc["file_id"], pinned["message_id"]
    return None, None


class Poller(threading.Thread):
    def __init__(self, handler):
        super().__init__(daemon=True)
        self.handler = handler
        self.offset = 0
        self.alive = True

    def run(self):
        while self.alive:
            try:
                updates = call("getUpdates", timeout=70,
                               offset=self.offset, **{"timeout": 30})
                for upd in updates:
                    self.offset = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("channel_post")
                    if msg:
                        try:
                            self.handler(msg)
                        except Exception as e:
                            print("tg handler:", e)
            except Exception as e:
                print("tg poll:", e)
                time.sleep(3)
