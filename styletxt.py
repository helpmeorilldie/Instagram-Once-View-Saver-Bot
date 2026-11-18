"""Unicode small-caps styling so every bot reply renders in the same voice."""

SMALL = {
    "a": "ᴀ", "b": "ʙ", "c": "ᴄ", "d": "ᴅ", "e": "ᴇ", "f": "ꜰ", "g": "ɢ", "h": "ʜ",
    "i": "ɪ", "j": "ᴊ", "k": "ᴋ", "l": "ʟ", "m": "ᴍ", "n": "ɴ", "o": "ᴏ", "p": "ᴘ",
    "q": "ǫ", "r": "ʀ", "s": "ꜱ", "t": "ᴛ", "u": "ᴜ", "v": "ᴠ", "w": "ᴡ", "x": "x",
    "y": "ʏ", "z": "ᴢ",
}

RULE = "──────────────────"


def sc(text: str) -> str:
    return "".join(SMALL.get(ch, ch) if ch.islower() else SMALL.get(ch.lower(), ch) if ch.isalpha() else ch
                   for ch in str(text))


def head(title: str) -> str:
    return f"❕ {sc(title)}"


def block(title: str, body: str) -> str:
    return f"{head(title)}\n{RULE}\n{body}"


def card(user: dict, status: str, status_label: str) -> str:
    return (
        f"🎗️ {sc('user profile')}\n"
        f"◌ {sc('username')} ⇝ @{user.get('username') or '—'}\n"
        f"◌ {sc('name')} ⇝ {user.get('full_name') or '—'}\n"
        f"◌ {sc('user id')} ⇝ {user.get('uid')}\n"
        f"◌ {sc('status')} ⇝ {status_label}\n"
        f"◌ {sc('saves')} ⇝ {user.get('saves', 0)}  •  {sc('dms')} ⇝ {user.get('msgs', 0)}\n"
        f"◌ {sc('last seen')} ⇝ {user.get('last_seen') or '—'}"
    )


STATUS = {
    "new": ("🆕", "ɴᴇᴡ"),
    "following": ("⏳", "ꜰᴏʟʟᴏᴡɪɴɢ"),
    "mutual": ("✅", "ᴍᴜᴛᴜᴀʟ"),
    "limited": ("🚧", "ᴅᴀɪʟʏ ʟɪᴍɪᴛ"),
    "premium": ("💎", "ᴘʀᴇᴍɪᴜᴍ"),
    "blocked": ("⛔", "ʙʟᴏᴄᴋᴇᴅ"),
}


def status_bits(status: str):
    return STATUS.get(status, ("❔", "ᴜɴᴋɴᴏᴡɴ"))
