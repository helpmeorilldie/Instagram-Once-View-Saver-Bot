# NoxMirage Saver

Session-cookie IG saver bot. Media and DB live in a private Telegram channel;
the host keeps only temp files.

## Setup
1. `pip install -r requirements.txt`
2. Copy `.env.example` → `.env`, fill `IG_SESSIONID`, `TG_TOKEN`, `TG_ADMIN_IDS`,
   `TG_STORAGE_CHANNEL`.
3. Create the private channel, add the control bot as **admin** (needed for pin).
4. `python main.py`

## Access
- Admin usernames from `EXTRA_ADMINS` are resolved to IDs on boot.
- Any group admin can run `.wlcum`; global admins can too.
- Mutual follow is required for `.eww`; follow-backs happen automatically.

## Commands
IG: `.eww` / `.save` (reply to any media), `.wlcum <text|reset>`
TG: `/dash /users /user /block /unblock /premium /db /help`

## Limits
`FREE_DAILY=10` applies to view-once + voice. Premium is stubbed via `/premium`.
