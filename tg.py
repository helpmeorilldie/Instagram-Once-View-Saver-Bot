"""Thin Telegram Bot API client: storage upload/download + admin control polling."""
import json
import threading
import time

import requests

import config as C

BASE = f"https://api.telegram.org/bot{C.TG_TOKEN}"
FILE_BASE = f"https://api.telegram.org/file/bot{C.TG_TOKEN}"


def _safe(e) -> str:
    """requests exceptions embed the full URL, which contains the bot token."""
    s = str(e)
    return s.replace(C.TG_TOKEN, "<token>") if C.TG_TOKEN else s


def _post(method, payload, http_timeout, file=None, attempts=4):
    """POST to the Bot API. Retries connection errors and 429 flood waits only,
    so a timed-out send is never blindly repeated."""
    last = None
    for attempt in range(attempts):
        fh = None
        try:
            kw = {"data": payload, "timeout": http_timeout}
            if file:
                field, path = file
                fh = open(path, "rb")
                kw["files"] = {field: (path.name, fh)}
            r = requests.post(f"{BASE}/{method}", **kw)
        except requests.ConnectionError as e:
            last = RuntimeError(f"telegram {method}: {_safe(e)}")
            time.sleep(1 + attempt)
            continue
        except requests.RequestException as e:
            raise RuntimeError(f"telegram {method}: {_safe(e)}") from None
        finally:
            if fh:
                fh.close()
        try:
            data = r.json()
        except ValueError:
            raise RuntimeError(f"telegram {method}: bad response ({r.status_code})") from None
        if data.get("ok"):
            return data["result"]
        wait = (data.get("parameters") or {}).get("retry_after")
        if data.get("error_code") == 429 and wait and attempt < attempts - 1:
            time.sleep(wait + 1)
            continue
        raise RuntimeError(f"telegram {method}: {data.get('description')}")
    raise last or RuntimeError(f"telegram {method}: failed")


def call(method: str, http_timeout: int = 60, **payload):
    return _post(method, payload, http_timeout)


def send_text(chat, text, reply_to=None, markup=None):
    payload = {"chat_id": chat, "text": text, "parse_mode": "HTML",
               "disable_web_page_preview": True}
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    if markup:
        payload["reply_markup"] = json.dumps(markup)
    try:
        return call("sendMessage", **payload)
    except RuntimeError as e:
        # A stray "<" in the text makes Telegram reject the whole message.
        # Fall back to plain text rather than dropping it.
        if "parse entities" not in str(e):
            raise
        payload.pop("parse_mode")
        return call("sendMessage", **payload)


def send_doc(chat, path, caption=""):
    return _post("sendDocument", {"chat_id": chat, "caption": caption[:1000]},
                 300, file=("document", path))


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
    # NOTE: the Bot API only lets bots download files up to 20 MB.
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
                # `timeout` is Telegram's long-poll seconds; the HTTP timeout
                # is a separate argument and must be larger.
                updates = call("getUpdates", http_timeout=45, timeout=30,
                               offset=self.offset,
                               allowed_updates=json.dumps(["message", "channel_post"]))
                for upd in updates:
                    self.offset = upd["update_id"] + 1
                    msg = upd.get("message") or upd.get("channel_post")
                    if msg:
                        try:
                            self.handler(msg)
                        except Exception as e:
                            print("tg handler:", _safe(e))
            except Exception as e:
                print("tg poll:", _safe(e))
                # 409 = another copy of the bot is polling with the same token
                time.sleep(10 if "Conflict" in str(e) else 3)
