# -*- coding: utf-8 -*-
"""
Telegram Business (Chat Automation) + Google Gemini + SQLite

- Business akkauntingizdagi barcha shaxsiy chatlarni kuzatadi:
  mijoz xabarlari, o'zingiz qo'lda yozgan xabarlar va botning javoblari
  `database.db` (SQLite) ga saqlanadi.
- Gemini butun suhbat tarixini (kontekstni) hisobga olib, tirik insondek javob yozadi.
- Siz chatda qo'lda yozsangiz, bot bir necha daqiqa jim turadi (OWNER_PAUSE_MINUTES).
- Chatda `/off` yozsangiz - shu chatda bot o'chadi, `/on` - yana yoqiladi.
  (Bu buyruqlar bazaga yozilmaydi va faqat SIZ yozganingizda ishlaydi.)

Muhit o'zgaruvchilari (Environment Variables):
  BOT_TOKEN            (majburiy)  @BotFather dan olingan token
  GEMINI_API_KEY       (majburiy)  Google AI Studio kaliti
  GEMINI_MODEL         (ixtiyoriy) default: gemini-2.5-flash
  DB_PATH              (ixtiyoriy) default: database.db  (Render Disk bilan: /data/database.db)
  PERSONA              (ixtiyoriy) AI uslubi/ko'rsatmasi (default matn pastda)
  HISTORY_LIMIT        (ixtiyoriy) AI ga beriladigan oxirgi xabarlar soni, default 40
  DEBOUNCE_SECONDS     (ixtiyoriy) ketma-ket xabarlarni kutish, default 3
  OWNER_PAUSE_MINUTES  (ixtiyoriy) o'zingiz yozgandan keyin jim turish, default 3 (0 = o'chirilgan)
  TIMEZONE             (ixtiyoriy) default: Asia/Tashkent
  PORT                 (Render o'zi beradi) health-check serveri uchun
"""

import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
import google.generativeai as genai
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import BusinessConnection, ErrorEvent, Message
from aiohttp import web
from google.generativeai.types import HarmBlockThreshold, HarmCategory

# --------------------------------------------------------------------------- #
#  SOZLAMALAR
# --------------------------------------------------------------------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
DB_PATH = os.getenv("DB_PATH", "database.db").strip() or "database.db"
PORT = int(os.getenv("PORT", "10000"))


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


HISTORY_LIMIT = max(4, _int_env("HISTORY_LIMIT", 40))
DEBOUNCE_SECONDS = max(0, _int_env("DEBOUNCE_SECONDS", 3))
OWNER_PAUSE_MINUTES = max(0, _int_env("OWNER_PAUSE_MINUTES", 3))

try:
    from zoneinfo import ZoneInfo

    TZ = ZoneInfo(os.getenv("TIMEZONE", "Asia/Tashkent"))
except Exception:  # tzdata yo'q bo'lsa
    TZ = timezone(timedelta(hours=5))

_primary_model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
GEMINI_MODELS: List[str] = []
for _m in (_primary_model, "gemini-2.5-flash", "gemini-2.0-flash", "gemini-flash-latest"):
    if _m not in GEMINI_MODELS:
        GEMINI_MODELS.append(_m)

DEFAULT_PERSONA = """Sen shu Telegram akkaunt egasining nomidan suhbatlashyapsan. Tirik odamdek: tabiiy, samimiy, iliq va qisqa yoz.

Qoidalar:
- Suhbatdosh qaysi tilda yozsa (o'zbek lotin/kirill, rus, ingliz), shu tilda va shu uslubda javob ber.
- Odatda 1-2 qisqa gap. Uzun ma'ruza, ro'yxat, sarlavha va markdown (*, #, `) ishlatma.
- Emoji juda kam, faqat joyi kelganda.
- "Sizga qanday yordam bera olaman", "men sun'iy intellektman" kabi robotcha iboralarni o'zing boshlama. Oldingi gaplarni takrorlama.
- Bilmagan narsani (narx, aniq vaqt, va'da, shaxsiy ma'lumot) o'ylab topma. Bunday hollarda "keyinroq aniq aytaman" kabi tabiiy javob ber.
- Suhbat tarixidagi "model" xabarlari seniki yoki akkaunt egasi yozganlari. Shu uslubni davom ettir, ziddiyat qilma.
- [rasm], [ovozli xabar], [video] kabi belgilar kelsa, sen u kontentni ko'ra/eshita olmaysan. Tabiiy ravishda nima ekanini so'ra.
- Agar odam jiddiy va to'g'ridan-to'g'ri "sen botmisan / AI misan?" deb so'rasa, yolg'on gapirma.
- Bir nechta alohida xabar tabiiyroq bo'lsa, ularni ||| belgisi bilan ajrat (ko'pi bilan 3 ta). Odatda bitta xabar yetarli."""

PERSONA = os.getenv("PERSONA", "").strip() or DEFAULT_PERSONA

ALLOWED_UPDATES = [
    "message",
    "business_connection",
    "business_message",
    "edited_business_message",
    "deleted_business_messages",
]

SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("business-bot")

dp = Dispatcher()

# --------------------------------------------------------------------------- #
#  MA'LUMOTLAR BAZASI (SQLite)
# --------------------------------------------------------------------------- #

DB: Optional[aiosqlite.Connection] = None
DB_WRITE_LOCK = asyncio.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT    NOT NULL,
    chat_id       INTEGER NOT NULL,
    message_id    INTEGER NOT NULL,
    sender_id     INTEGER,
    sender_name   TEXT,
    role          TEXT    NOT NULL,      -- 'user' | 'owner' | 'bot'
    content_type  TEXT,
    text          TEXT,
    ts            INTEGER NOT NULL,      -- unix vaqt (UTC)
    edited        INTEGER NOT NULL DEFAULT 0,
    deleted       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (connection_id, chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (chat_id, ts, id);

CREATE TABLE IF NOT EXISTS connections (
    connection_id TEXT PRIMARY KEY,
    owner_id      INTEGER,
    owner_name    TEXT,
    is_enabled    INTEGER NOT NULL DEFAULT 1,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS chat_state (
    chat_id     INTEGER PRIMARY KEY,
    bot_enabled INTEGER NOT NULL DEFAULT 1
);
"""


async def db_init() -> None:
    global DB
    folder = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(folder, exist_ok=True)
    DB = await aiosqlite.connect(DB_PATH)
    DB.row_factory = aiosqlite.Row
    await DB.execute("PRAGMA journal_mode=WAL")
    await DB.execute("PRAGMA synchronous=NORMAL")
    await DB.executescript(SCHEMA)
    await DB.commit()
    log.info("Baza tayyor: %s", os.path.abspath(DB_PATH))


async def db_execute(sql: str, params: Tuple[Any, ...] = ()) -> None:
    assert DB is not None
    async with DB_WRITE_LOCK:
        await DB.execute(sql, params)
        await DB.commit()


async def db_fetchone(sql: str, params: Tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
    assert DB is not None
    async with DB.execute(sql, params) as cur:
        return await cur.fetchone()


async def db_fetchall(sql: str, params: Tuple[Any, ...] = ()) -> List[aiosqlite.Row]:
    assert DB is not None
    async with DB.execute(sql, params) as cur:
        return list(await cur.fetchall())


async def save_message(
    *,
    connection_id: str,
    chat_id: int,
    message_id: int,
    sender_id: Optional[int],
    sender_name: Optional[str],
    role: str,
    content_type: str,
    text: str,
    ts: int,
    force: bool = False,
) -> None:
    """Xabarni saqlaydi. force=True bo'lsa mavjud yozuvni yangilaydi (bot javoblari uchun)."""
    try:
        if force:
            sql = (
                "INSERT INTO messages (connection_id, chat_id, message_id, sender_id, sender_name, "
                "role, content_type, text, ts) VALUES (?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(connection_id, chat_id, message_id) DO UPDATE SET "
                "role=excluded.role, sender_id=excluded.sender_id, sender_name=excluded.sender_name, "
                "content_type=excluded.content_type, text=excluded.text"
            )
        else:
            sql = (
                "INSERT OR IGNORE INTO messages (connection_id, chat_id, message_id, sender_id, "
                "sender_name, role, content_type, text, ts) VALUES (?,?,?,?,?,?,?,?,?)"
            )
        await db_execute(
            sql,
            (connection_id, chat_id, message_id, sender_id, sender_name, role, content_type, text, ts),
        )
    except Exception:
        log.exception("Xabarni bazaga yozishda xato")


async def is_bot_enabled(chat_id: int) -> bool:
    try:
        row = await db_fetchone("SELECT bot_enabled FROM chat_state WHERE chat_id=?", (chat_id,))
        return True if row is None else bool(row["bot_enabled"])
    except Exception:
        log.exception("chat_state o'qishda xato")
        return True


async def set_bot_enabled(chat_id: int, enabled: bool) -> None:
    await db_execute(
        "INSERT INTO chat_state (chat_id, bot_enabled) VALUES (?,?) "
        "ON CONFLICT(chat_id) DO UPDATE SET bot_enabled=excluded.bot_enabled",
        (chat_id, 1 if enabled else 0),
    )


async def upsert_connection(conn_id: str, owner_id: Optional[int], owner_name: Optional[str], enabled: bool) -> None:
    await db_execute(
        "INSERT INTO connections (connection_id, owner_id, owner_name, is_enabled, updated_at) "
        "VALUES (?,?,?,?,?) ON CONFLICT(connection_id) DO UPDATE SET "
        "owner_id=excluded.owner_id, owner_name=excluded.owner_name, "
        "is_enabled=excluded.is_enabled, updated_at=excluded.updated_at",
        (conn_id, owner_id, owner_name, 1 if enabled else 0, int(time.time())),
    )


async def get_owner_name(bot: Bot, conn_id: str) -> Optional[str]:
    try:
        row = await db_fetchone("SELECT owner_name FROM connections WHERE connection_id=?", (conn_id,))
        if row and row["owner_name"]:
            return row["owner_name"]
        bc = await bot.get_business_connection(business_connection_id=conn_id)
        name = bc.user.full_name
        await upsert_connection(conn_id, bc.user.id, name, bool(bc.is_enabled))
        return name
    except Exception:
        log.warning("Akkaunt egasi ismini aniqlab bo'lmadi", exc_info=True)
        return None


async def load_history(chat_id: int) -> List[aiosqlite.Row]:
    return await db_fetchall(
        "SELECT role, text FROM ("
        "  SELECT id, ts, role, text FROM messages "
        "  WHERE chat_id=? AND deleted=0 AND text IS NOT NULL AND text<>'' "
        "  ORDER BY ts DESC, id DESC LIMIT ?"
        ") ORDER BY ts ASC, id ASC",
        (chat_id, HISTORY_LIMIT),
    )


# --------------------------------------------------------------------------- #
#  YORDAMCHI FUNKSIYALAR
# --------------------------------------------------------------------------- #

CONTENT_LABELS = {
    "photo": "[rasm]",
    "video": "[video]",
    "video_note": "[dumaloq video]",
    "voice": "[ovozli xabar]",
    "audio": "[audio]",
    "animation": "[gif]",
    "sticker": "[stiker]",
    "document": "[fayl]",
    "location": "[joylashuv]",
    "venue": "[joy]",
    "contact": "[kontakt]",
    "poll": "[so'rovnoma]",
    "dice": "[o'yin kubigi]",
    "story": "[hikoya]",
}


def describe_message(message: Message) -> Tuple[str, str]:
    """(content_type, saqlanadigan matn) qaytaradi."""
    ctype = str(getattr(message.content_type, "value", message.content_type))
    text = (message.text or message.caption or "").strip()
    if ctype == "text":
        return ctype, text

    label = CONTENT_LABELS.get(ctype, f"[{ctype}]")
    try:
        if ctype == "sticker" and message.sticker and message.sticker.emoji:
            label = f"[stiker {message.sticker.emoji}]"
        elif ctype == "document" and message.document and message.document.file_name:
            label = f"[fayl: {message.document.file_name}]"
    except Exception:
        pass
    return ctype, f"{label} {text}".strip()


def build_contents(rows: List[aiosqlite.Row]) -> List[Dict[str, Any]]:
    """Bazadagi tarixni Gemini formatiga o'giradi (ketma-ket bir xil rollar birlashtiriladi)."""
    merged: List[Dict[str, Any]] = []
    for r in rows:
        text = (r["text"] or "").strip()
        if not text:
            continue
        role = "user" if r["role"] == "user" else "model"
        if merged and merged[-1]["role"] == role:
            merged[-1]["parts"][0] += "\n" + text
        else:
            merged.append({"role": role, "parts": [text]})
    while merged and merged[0]["role"] != "user":
        merged.pop(0)
    return merged


def build_system_prompt(owner_name: Optional[str], client_name: str) -> str:
    now = datetime.now(TZ).strftime("%Y-%m-%d %H:%M, %A")
    owner_line = (
        f"Sen {owner_name} nomidan yozyapsan." if owner_name else "Sen akkaunt egasi nomidan yozyapsan."
    )
    return (
        f"{PERSONA}\n\n"
        f"Qo'shimcha kontekst:\n"
        f"- {owner_line}\n"
        f"- Suhbatdosh: {client_name}\n"
        f"- Hozirgi vaqt: {now}\n"
        f"- Faqat javob matnining o'zini yoz (ism, 'Men:' kabi prefikslarsiz)."
    )


def extract_text(resp: Any) -> str:
    try:
        return (resp.text or "").strip()
    except Exception:
        pass
    parts: List[str] = []
    try:
        for cand in getattr(resp, "candidates", None) or []:
            content = getattr(cand, "content", None)
            for p in getattr(content, "parts", None) or []:
                t = getattr(p, "text", None)
                if t:
                    parts.append(t)
    except Exception:
        pass
    return "".join(parts).strip()


def clean_answer(text: str) -> str:
    text = text.replace("**", "").replace("__", "").replace("`", "")
    return text.strip()


def split_answer(text: str) -> List[str]:
    parts = [p.strip() for p in text.split("|||") if p.strip()]
    if not parts:
        return []
    if len(parts) > 3:
        parts = parts[:2] + [" ".join(parts[2:])]
    return [p[:4000] for p in parts]


async def ask_gemini(system_prompt: str, contents: List[Dict[str, Any]]) -> Optional[str]:
    for model_name in GEMINI_MODELS:
        for attempt in range(2):
            try:
                model = genai.GenerativeModel(
                    model_name=model_name,
                    system_instruction=system_prompt,
                    generation_config=genai.GenerationConfig(
                        temperature=0.9,
                        top_p=0.95,
                        max_output_tokens=500,
                    ),
                    safety_settings=SAFETY_SETTINGS,
                )
                resp = await asyncio.wait_for(model.generate_content_async(contents), timeout=45)
                text = clean_answer(extract_text(resp))
                if text:
                    return text
                log.warning("Gemini (%s) bo'sh javob qaytardi", model_name)
                break  # keyingi modelga o'tamiz
            except asyncio.TimeoutError:
                log.warning("Gemini (%s) vaqt tugadi (urinish %d)", model_name, attempt + 1)
            except Exception as e:
                log.warning("Gemini (%s) xatosi (urinish %d): %s", model_name, attempt + 1, e)
                await asyncio.sleep(1.5 * (attempt + 1))
    return None


async def send_typing(bot: Bot, chat_id: int, conn_id: str) -> None:
    try:
        await bot.send_chat_action(chat_id=chat_id, action="typing", business_connection_id=conn_id)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
#  JAVOB BERISH (debounce + lock)
# --------------------------------------------------------------------------- #

_versions: Dict[int, int] = {}
_locks: Dict[int, asyncio.Lock] = {}
_tasks: set = set()


def schedule_reply(bot: Bot, chat_id: int, conn_id: str) -> None:
    version = _versions.get(chat_id, 0) + 1
    _versions[chat_id] = version
    task = asyncio.create_task(reply_worker(bot, chat_id, conn_id, version))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def reply_worker(bot: Bot, chat_id: int, conn_id: str, version: int) -> None:
    try:
        # Mijoz ketma-ket yozayotgan bo'lsa, tugashini kutamiz
        await asyncio.sleep(DEBOUNCE_SECONDS)
        if _versions.get(chat_id) != version:
            return

        lock = _locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            if _versions.get(chat_id) != version:
                return
            if not await is_bot_enabled(chat_id):
                return

            last = await db_fetchone(
                "SELECT role, sender_name FROM messages WHERE chat_id=? AND deleted=0 "
                "ORDER BY ts DESC, id DESC LIMIT 1",
                (chat_id,),
            )
            if not last or last["role"] != "user":
                return  # oxirgi xabar mijozniki emas (masalan, o'zingiz javob yozgansiz)

            if OWNER_PAUSE_MINUTES > 0:
                row = await db_fetchone(
                    "SELECT MAX(ts) AS t FROM messages WHERE chat_id=? AND role='owner'", (chat_id,)
                )
                if row and row["t"] and time.time() - row["t"] < OWNER_PAUSE_MINUTES * 60:
                    log.info("Chat %s: egasi yaqinda yozgan, bot jim turadi", chat_id)
                    return

            contents = build_contents(await load_history(chat_id))
            if not contents or contents[-1]["role"] != "user":
                return

            owner_name = await get_owner_name(bot, conn_id)
            client_name = (last["sender_name"] or "Suhbatdosh").strip() or "Suhbatdosh"
            system_prompt = build_system_prompt(owner_name, client_name)

            await send_typing(bot, chat_id, conn_id)
            answer = await ask_gemini(system_prompt, contents)
            if not answer:
                log.error("Chat %s: Gemini javob bermadi, xabar yuborilmadi", chat_id)
                return

            if _versions.get(chat_id) != version:
                return  # generatsiya paytida yangi xabar keldi, yangisi javob beradi

            for i, part in enumerate(split_answer(answer)):
                await send_typing(bot, chat_id, conn_id)
                await asyncio.sleep(min(max(len(part) * 0.05, 0.8), 5.0))
                if _versions.get(chat_id) != version:
                    return
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=part,
                    business_connection_id=conn_id,
                )
                await save_message(
                    connection_id=conn_id,
                    chat_id=chat_id,
                    message_id=sent.message_id,
                    sender_id=None,
                    sender_name="AI",
                    role="bot",
                    content_type="text",
                    text=part,
                    ts=int(time.time()),
                    force=True,
                )
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Chat %s: javob berishda xato", chat_id)


# --------------------------------------------------------------------------- #
#  HANDLERLAR
# --------------------------------------------------------------------------- #


@dp.business_connection()
async def on_business_connection(bc: BusinessConnection) -> None:
    try:
        await upsert_connection(bc.id, bc.user.id, bc.user.full_name, bool(bc.is_enabled))
        log.info("Business ulanish yangilandi: %s (yoqilgan=%s)", bc.user.full_name, bc.is_enabled)
    except Exception:
        log.exception("business_connection ni qayta ishlashda xato")


@dp.business_message()
async def on_business_message(message: Message, bot: Bot) -> None:
    try:
        conn_id = message.business_connection_id
        chat_id = message.chat.id
        sender = message.from_user
        if not conn_id or sender is None:
            return

        # Rolni aniqlash: shaxsiy chatda chat.id == mijoz id si
        if getattr(message, "sender_business_bot", None) is not None:
            role = "bot"
        elif sender.id == chat_id:
            role = "user"
        else:
            role = "owner"

        ctype, text = describe_message(message)

        # Faqat egasi uchun boshqaruv buyruqlari
        if role == "owner" and text.lower() in ("/off", "/on"):
            await set_bot_enabled(chat_id, text.lower() == "/on")
            log.info("Chat %s: bot %s", chat_id, "yoqildi" if text.lower() == "/on" else "o'chirildi")
            return

        await save_message(
            connection_id=conn_id,
            chat_id=chat_id,
            message_id=message.message_id,
            sender_id=sender.id,
            sender_name=sender.full_name,
            role=role,
            content_type=ctype,
            text=text,
            ts=int(message.date.timestamp()),
        )

        if role == "user" and not sender.is_bot:
            schedule_reply(bot, chat_id, conn_id)
    except Exception:
        log.exception("business_message ni qayta ishlashda xato")


@dp.edited_business_message()
async def on_edited_business_message(message: Message) -> None:
    try:
        if not message.business_connection_id:
            return
        _, text = describe_message(message)
        await db_execute(
            "UPDATE messages SET text=?, edited=1 WHERE connection_id=? AND chat_id=? AND message_id=?",
            (text, message.business_connection_id, message.chat.id, message.message_id),
        )
    except Exception:
        log.exception("edited_business_message da xato")


@dp.deleted_business_messages()
async def on_deleted_business_messages(event) -> None:
    try:
        conn_id = event.business_connection_id
        chat_id = event.chat.id
        for mid in event.message_ids:
            await db_execute(
                "UPDATE messages SET deleted=1 WHERE connection_id=? AND chat_id=? AND message_id=?",
                (conn_id, chat_id, mid),
            )
    except Exception:
        log.exception("deleted_business_messages da xato")


@dp.message(CommandStart())
async def on_start(message: Message) -> None:
    try:
        await message.answer(
            "Salom! Men Telegram Business orqali ishlaydigan avto-javob botiman.\n\n"
            "Ulash: Telegram > Sozlamalar > Telegram Business > Chatbotlar, "
            "so'ng meni tanlang va 'xabarlarga javob berish' ruxsatini bering.\n"
            "Biror chatda /off yozsangiz, shu chatda o'chadi; /on yozsangiz, yana yoqiladi."
        )
    except Exception:
        log.exception("/start da xato")


@dp.errors()
async def on_error(event: ErrorEvent) -> bool:
    log.error("Kutilmagan xato: %s", event.exception, exc_info=event.exception)
    return True


# --------------------------------------------------------------------------- #
#  HEALTH-CHECK SERVER (Render uchun)
# --------------------------------------------------------------------------- #


async def health(_: web.Request) -> web.Response:
    return web.Response(text="OK")


async def start_web_server() -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("Health server %s-portda ishga tushdi", PORT)
    return runner


# --------------------------------------------------------------------------- #
#  ISHGA TUSHIRISH
# --------------------------------------------------------------------------- #


async def main() -> None:
    if not BOT_TOKEN:
        log.critical("BOT_TOKEN o'rnatilmagan!")
        sys.exit(1)
    if not GEMINI_API_KEY:
        log.critical("GEMINI_API_KEY o'rnatilmagan!")
        sys.exit(1)

    genai.configure(api_key=GEMINI_API_KEY)
    await db_init()
    runner = await start_web_server()
    bot = Bot(token=BOT_TOKEN)

    try:
        me = await bot.get_me()
        log.info("Bot ishga tushdi: @%s", me.username)
        try:
            await bot.delete_webhook(drop_pending_updates=False)
        except Exception:
            log.warning("Webhook o'chirishda xato", exc_info=True)

        while True:
            try:
                await dp.start_polling(bot, allowed_updates=ALLOWED_UPDATES)
                break  # to'xtatish signali bilan normal chiqish
            except (KeyboardInterrupt, asyncio.CancelledError):
                raise
            except Exception:
                log.exception("Polling to'xtadi, 5 soniyadan keyin qayta uriniladi")
                await asyncio.sleep(5)
    finally:
        try:
            await bot.session.close()
        except Exception:
            pass
        try:
            await runner.cleanup()
        except Exception:
            pass
        if DB is not None:
            try:
                await DB.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("To'xtatildi")
