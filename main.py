import signal
import threading
import time
import traceback

import config as C
import db
import igbot
import store
import styletxt as S
import tg
from adminbot import handle


def resolve_admins(cl):
    for name in C.ADMIN_USERNAMES:
        try:
            uid = cl.user_id_from_username(name)
            C.BOT_ADMIN_UIDS.add(int(uid))
            print("admin resolved:", name, uid)
        except Exception as e:
            print("admin resolve failed:", name, e)


def boot():
    C.DATA_DIR.mkdir(parents=True, exist_ok=True)
    C.TMP_DIR.mkdir(parents=True, exist_ok=True)

    if store.restore_db():
        print("db restored from telegram")

    db.init()

    cl = igbot.build_client()
    cl.user_id = cl.user_id
    resolve_admins(cl)

    if C.TG_TOKEN:
        tg.Poller(handle).start()

    threading.Thread(target=igbot.follow_back_loop, args=(cl,), daemon=True).start()

    if C.REALTIME:
        threading.Thread(target=igbot.realtime_thread, args=(cl,), daemon=True).start()

    seen_users = {}
    try:
        igbot.poll(cl, process=C.PROCESS_BACKLOG, seen_users=seen_users)
    except Exception:
        print(traceback.format_exc())

    last_snap = last_ka = time.time()
    while not igbot._stop.is_set():
        igbot._wake.wait(C.POLL_INTERVAL)
        igbot._wake.clear()
        try:
            igbot.poll(cl, seen_users=seen_users)
        except Exception as e:
            print("poll:", e)
            time.sleep(5)
        if time.time() - last_snap > 300:
            try:
                store.snapshot_db()
            except Exception as e:
                print("snapshot:", e)
            last_snap = time.time()
        if time.time() - last_ka > 600:
            try:
                cl.get_timeline_feed()
            except Exception:
                pass
            last_ka = time.time()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, lambda *_: igbot._stop.set())
    signal.signal(signal.SIGTERM, lambda *_: igbot._stop.set())
    boot()
