import time

import config as C
import db
import store
import styletxt as S
import tg


def dashboard():
    s = db.stats()
    lines = [
        S.head("dashboard"),
        S.RULE,
        f"👥 {S.sc('users')} ⇝ {s['users']}   ✅ {S.sc('mutual')} ⇝ {s['mutual']}",
        f"📥 {S.sc('saves')} ⇝ {s['saves']}   💬 {S.sc('messages')} ⇝ {s['dms']}",
        f"🟢 {S.sc('active 24h')} ⇝ {s['active24']}",
        S.RULE,
    ]
    for e in db.events(8):
        emoji, label = S.status_bits((db.get_user(e["uid"]) or {"status": "new"})["status"]) if e["uid"] else ("❔", "—")
        who = f"@{e['username']}" if e["username"] else e["uid"]
        lines.append(f"{emoji} {who} • {S.sc(e['kind'])} • {time.strftime('%H:%M', time.localtime(e['ts']))}")
    return "\n".join(lines)


def handle(msg):
    if msg["chat"]["id"] not in C.TG_ADMIN_IDS:
        return
    text = (msg.get("text") or "").strip()
    if not text.startswith("/"):
        return
    cmd, _, arg = text.partition(" ")
    cmd = cmd.split("@")[0].lower()

    if cmd == "/dash":
        tg.send_text(msg["chat"]["id"], dashboard())

    elif cmd == "/users":
        rows = db.many("SELECT * FROM users ORDER BY last_seen DESC LIMIT 15")
        if not rows:
            return tg.send_text(msg["chat"]["id"], "ɴᴏ ᴜꜱᴇʀꜱ ʏᴇᴛ.")
        out = [S.head("recent users"), S.RULE]
        for r in rows:
            emoji, label = S.status_bits(r["status"])
            out.append(f"{emoji} @{r['username'] or '—'} • {label} • {r['saves']}💾")
        tg.send_text(msg["chat"]["id"], "\n".join(out))

    elif cmd == "/user" and arg:
        u = db.get_user(int(arg)) if arg.isdigit() else db.by_username(arg)
        if not u:
            return tg.send_text(msg["chat"]["id"], "ɴᴏᴛ ꜰᴏᴜɴᴅ.")
        emoji, label = S.status_bits(u["status"])
        tg.send_text(msg["chat"]["id"], S.card(dict(u), u["status"], label))

    elif cmd in ("/block", "/unblock", "/premium") and arg:
        u = db.get_user(int(arg)) if arg.isdigit() else db.by_username(arg)
        if not u:
            return tg.send_text(msg["chat"]["id"], "ɴᴏᴛ ꜰᴏᴜɴᴅ.")
        new = {"/block": "blocked", "/unblock": "mutual", "/premium": "premium"}[cmd]
        db.set_status(u["uid"], new)
        tg.send_text(msg["chat"]["id"], f"@{u['username']} ⇝ {new}")

    elif cmd == "/db":
        store.snapshot_db()
        tg.send_text(msg["chat"]["id"], "ᴅᴀᴛᴀʙᴀꜱᴇ ꜱɴᴀᴘꜱʜᴏᴛ ᴘɪɴɴᴇᴅ 🔒")

    elif cmd == "/help":
        tg.send_text(msg["chat"]["id"], "\n".join([
            S.head("commands"), S.RULE,
            "/dash — ʟɪᴠᴇ ᴅᴀꜱʜʙᴏᴀʀᴅ",
            "/users — ʀᴇᴄᴇɴᴛ ᴜꜱᴇʀꜱ",
            "/user <@name|id> — ᴘʀᴏꜰɪʟᴇ",
            "/block /unblock /premium <@name|id>",
            "/db — ꜱɴᴀᴘꜱʜᴏᴛ + ᴘɪɴ",
        ]))
