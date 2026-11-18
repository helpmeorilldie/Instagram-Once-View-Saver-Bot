import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _b(key, default="0"):
    return os.environ.get(key, default).strip() in ("1", "true", "yes", "on")


IG_SESSIONID = os.environ.get("IG_SESSIONID", "").strip()
TRIGGER = os.environ.get("TRIGGER", ".eww").strip().lower()
ALIASES = {TRIGGER, ".save", ".s"}
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "3"))
PROCESS_BACKLOG = _b("PROCESS_BACKLOG")
REALTIME = _b("REALTIME", "1")
DRY_RUN = _b("DRY_RUN")
MAX_BYTES = int(float(os.environ.get("MAX_MB", "60")) * 1024 * 1024)
DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))

TG_TOKEN = os.environ.get("TG_TOKEN", "").strip()
TG_ADMIN_IDS = {int(x) for x in os.environ.get("TG_ADMIN_IDS", "").replace(" ", "").split(",") if x}
TG_CONTROL_CHAT = os.environ.get("TG_CONTROL_CHAT", "").strip()
TG_STORAGE_CHANNEL = os.environ.get("TG_STORAGE_CHANNEL", "").strip()
TG_DB_CHANNEL = os.environ.get("TG_DB_CHANNEL", "").strip() or TG_STORAGE_CHANNEL

FREE_DAILY = int(os.environ.get("FREE_DAILY", "10"))

ADMIN_USERNAMES = [u.strip().lstrip("@").lower()
                   for u in os.environ.get("EXTRA_ADMINS", "ammar._.azam,noxmirage_").split(",")
                   if u.strip()]

BOT_ADMIN_UIDS: set[int] = set()

DB_FILE = DATA_DIR / "bot.sqlite"
TMP_DIR = DATA_DIR / "tmp"
UA = "Instagram 269.0.0.18.75 Android"
