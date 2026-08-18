import os
import sys
import asyncio
import json
import logging
import asyncpg
import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton, 
    CallbackQuery, BufferedInputFile
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from dotenv import load_dotenv

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# --- КОНФИГУРАЦИЯ ---
TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

R34_USER_ID = os.getenv("R34_USER_ID", "").strip()
R34_API_KEY = os.getenv("R34_API_KEY", "").strip()
GELBOORU_USER_ID = os.getenv("GELBOORU_USER_ID", "").strip()
GELBOORU_API_KEY = os.getenv("GELBOORU_API_KEY", "").strip()

ALLOWED_USERS = []
for i in range(1, 4):
    uid = os.getenv(f"ALLOWED_USER_{i}")
    if uid and uid.isdigit():
        ALLOWED_USERS.append(int(uid))

if not TOKEN:
    logger.error("❌ BOT_TOKEN не найден в .env!")
    exit(1)

def check_access(user_id: int) -> bool:
    return not ALLOWED_USERS or user_id in ALLOWED_USERS

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ МОДУЛЯ ---
MODERATION_CHAT_ID = None
PARSER_ENABLED = True
PARSER_SPEED = 15
PARSER_LIMIT = 20
LABELS = []

db_pool = None
post_metadata_store = {}  # Временное хранение метаданных сообщений модерации

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# --- DATABASE (POSTGRESQL) ---

async def init_db():
    global db_pool
    if not DATABASE_URL:
        logger.warning("⚠️ DATABASE_URL не указан. База данных отключена.")
        return

    url = DATABASE_URL.replace("postgres://", "postgresql://", 1) if DATABASE_URL.startswith("postgres://") else DATABASE_URL

    try:
        db_pool = await asyncpg.create_pool(dsn=url)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS parser_config (
                    id INT PRIMARY KEY DEFAULT 1,
                    mod_chat_id BIGINT,
                    labels JSONB NOT NULL DEFAULT '[]'::jsonb,
                    queue JSONB NOT NULL DEFAULT '[]'::jsonb,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS seen_posts (
                    source VARCHAR(20) NOT NULL,
                    post_id VARCHAR(50) NOT NULL,
                    file_md5 VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source, post_id)
                );
            """)
        logger.info("✅ База данных PostgreSQL успешно инициализирована!")
    except Exception as e:
        logger.error(f"❌ Ошибка инициализации PostgreSQL: {e}")

async def load_state():
    global MODERATION_CHAT_ID, LABELS
    if not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT mod_chat_id, labels FROM parser_config WHERE id = 1;")
            if row:
                MODERATION_CHAT_ID = row["mod_chat_id"]
                lbls = row["labels"]
                LABELS = json.loads(lbls) if isinstance(lbls, str) else (lbls or [])
        logger.info(f"✅ Состояние загружено. Группа: {MODERATION_CHAT_ID}, Лейблов: {len(LABELS)}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки состояния: {e}")

async def save_state():
    if not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parser_config (id, mod_chat_id, labels, updated_at)
                VALUES (1, $1, $2::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE 
                SET mod_chat_id = EXCLUDED.mod_chat_id, labels = EXCLUDED.labels, updated_at = CURRENT_TIMESTAMP;
            """, MODERATION_CHAT_ID, json.dumps(LABELS, ensure_ascii=False))
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения состояния: {e}")

async def load_queue_db():
    if not db_pool: return []
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT queue FROM parser_config WHERE id = 1;")
            if row and row["queue"]:
                q = row["queue"]
                return json.loads(q) if isinstance(q, str) else q
    except Exception as e:
        logger.error(f"❌ Ошибка чтения очереди: {e}")
    return []

async def save_queue_db(queue_data):
    if not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO parser_config (id, queue, updated_at)
                VALUES (1, $1::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (id) DO UPDATE SET queue = EXCLUDED.queue, updated_at = CURRENT_TIMESTAMP;
            """, json.dumps(queue_data, ensure_ascii=False))
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения очереди: {e}")

async def is_post_seen(source: str, post_id: str) -> bool:
    if not db_pool: return False
    try:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT 1 FROM seen_posts WHERE source=$1 AND post_id=$2", source, str(post_id))
            return row is not None
    except Exception: return False

async def is_md5_seen(file_md5: str):
    if not db_pool or not file_md5: return None
    try:
        async with db_pool.acquire() as conn:
            return await conn.fetchrow("SELECT source, post_id FROM seen_posts WHERE file_md5=$1", str(file_md5))
    except Exception: return None

async def mark_post_seen(source: str, post_id: str, file_md5: str = None):
    if not db_pool: return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO seen_posts (source, post_id, file_md5)
                VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;
            """, source, str(post_id), file_md5)
    except Exception: pass

# --- МОДУЛЬ ПАРСИНГА ---

async def fetch_booru_posts(source: str, tags: str, limit: int = 20):
    posts = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    clean_tags = tags.strip().replace(" ", "+")
    params = {"page": "dapi", "s": "post", "q": "index", "json": "1", "tags": clean_tags, "limit": str(limit)}

    if source == "rule34":
        base_url = "https://api.rule34.xxx/index.php"
        if R34_USER_ID and R34_API_KEY:
            params["user_id"], params["api_key"] = R34_USER_ID, R34_API_KEY
    elif source == "gelbooru":
        base_url = "https://gelbooru.com/index.php"
        if GELBOORU_USER_ID and GELBOORU_API_KEY:
            params["user_id"], params["api_key"] = GELBOORU_USER_ID, GELBOORU_API_KEY
    else: return posts

    try:
        await asyncio.sleep(0.5)
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params, headers=headers, timeout=10) as resp:
                if resp.status == 200:
                    text_data = await resp.text()
                    if text_data.strip():
                        data = json.loads(text_data)
                        posts = data if isinstance(data, list) else data.get("post", [])
                elif resp.status == 429:
                    logger.warning(f"⚠️ {source} ответил 429 (Too Many Requests). Пауза 3 сек.")
                    await asyncio.sleep(3)
    except Exception as e:
        logger.error(f"❌ Ошибка запроса {source} ({clean_tags}): {e}")

    return posts

async def process_parsed_post(label: dict, post_data: dict, source: str):
    post_id = str(post_data.get("id"))
    file_url = post_data.get("file_url")
    file_md5 = post_data.get("md5") or post_data.get("hash")

    if not file_url or not post_id: return
    if await is_post_seen(source, post_id): return

    duplicate_info = await is_md5_seen(file_md5)
    await mark_post_seen(source, post_id, file_md5)

    source_name = "🟡 Gelbooru" if source == "gelbooru" else "🟢 Rule34"
    source_link = f'<a href="https://{source}.xxx/index.php?page=post&s=view&id={post_id}">{source_name}</a>'
    
    caption = f"🏷 <b>Лейбл:</b> {label.get('emoji', '🏷')} {label.get('name')}\n"
    if duplicate_info:
        caption = f"⚠️ <b>ВОЗМОЖНЫЙ ДУБЛИКАТ</b> (уже выходил в {duplicate_info['source']})\n" + caption

    caption += f"🔗 <b>Источник:</b> {source_link}\n🆔 <code>{post_id}</code>"
    custom_sig = label.get("signature", "")

    # ТОЛЬКО 2 БЕЗОПАСНЫЕ КНОПКИ В ГРУППЕ
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📥 В очередь", callback_data=f"mod:queue:{source}:{post_id}"),
            InlineKeyboardButton(text="❌ Скрыть", callback_data="mod:delete")
        ]
    ])

    mode = label.get("mode", "MANUAL")
    if mode == "AUTO" and not duplicate_info:
        queue = await load_queue_db()
        queue.append({"file_id": file_url, "type": "photo", "caption": custom_sig})
        await save_queue_db(queue)
        logger.info(f"⚡ [AUTO] Пост #{post_id} авто-добавлен в очередь!")
        return

    if MODERATION_CHAT_ID:
        try:
            msg = await bot.send_photo(
                chat_id=MODERATION_CHAT_ID,
                photo=file_url,
                caption=caption,
                reply_markup=kb
            )
            post_metadata_store[f"mod_{msg.message_id}"] = {
                "file_url": file_url,
                "caption": custom_sig,
                "source": source,
                "post_id": post_id
            }
        except Exception as e:
            logger.error(f"❌ Ошибка отправки карточки в группу: {e}")

async def parser_loop():
    logger.info("🚀 Фоновый парсер запущен!")
    while True:
        try:
            if PARSER_ENABLED and LABELS:
                for label in LABELS:
                    sources = label.get("sources", ["rule34", "gelbooru"])
                    tags = label.get("tags", "")
                    if not tags: continue

                    for src in sources:
                        posts = await fetch_booru_posts(src, tags, limit=PARSER_LIMIT)
                        for post in posts:
                            await process_parsed_post(label, post, src)
                            await asyncio.sleep(0.2)

            await asyncio.sleep(max(1, PARSER_SPEED))
        except (asyncio.CancelledError, Exception) as e:
            logger.error(f"❌ Ошибка в parser_loop: {e}")
            await asyncio.sleep(10)

# --- ОБРАБОТЧИКИ КНОПОК В ГРУППЕ МОДЕРАЦИИ ---

@dp.callback_query(F.data.startswith("mod:"))
async def handle_mod_callback(callback: CallbackQuery):
    if not check_access(callback.from_user.id):
        await callback.answer("У вас нет прав.", show_alert=True)
        return

    action = callback.data.split(":")[1]

    if action == "delete":
        try: await callback.message.delete()
        except: pass
        await callback.answer("Скрыто!")
        return

    if action == "queue":
        key = f"mod_{callback.message.message_id}"
        meta = post_metadata_store.get(key, {})
        file_url = meta.get("file_url") or (callback.message.photo[-1].file_id if callback.message.photo else None)
        caption = meta.get("caption", "")

        if not file_url:
            await callback.answer("❌ Ошибка получения файла.", show_alert=True)
            return

        queue = await load_queue_db()
        queue.append({"file_id": file_url, "type": "photo", "caption": caption})
        await save_queue_db(queue)

        await callback.message.edit_caption(
            caption=callback.message.caption + "\n\n✅ <b>ОДОБРЕНО И ДОБАВЛЕНО В ОЧЕРЕДЬ</b>"
        )
        await callback.answer("Добавлено в очередь!")

# --- ОБРАБОТЧИКИ В ЛИЧКЕ БОТА (ДАБЛЧЕК И ОЧЕРЕДЬ) ---

@dp.message(F.text == "/queue")
async def show_queue_cmd(message: Message):
    if not check_access(message.from_user.id): return
    if message.chat.type != "private":
        await message.reply("Эту команду можно вызывать только в ЛС!")
        return

    queue = await load_queue_db()
    if not queue:
        await message.reply("📭 Очередь публикаций пуста.")
        return

    await message.reply(f"📊 <b>Постов в очереди: {len(queue)} шт.</b>\nПрисылаю первые 5 постов с даблчек-кнопками:")

    for idx, item in enumerate(queue[:5]):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🚀 Запостить сейчас", callback_data=f"askpost:{idx}"),
                InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delq:{idx}")
            ]
        ])
        
        caption = f"📌 <b>Пост #{idx + 1} в очереди</b>\n" + (item.get("caption") or "")
        file_id = item.get("file_id")

        try:
            await message.answer_photo(photo=file_id, caption=caption, reply_markup=kb)
        except Exception:
            await message.answer(f"📌 <b>Пост #{idx + 1}</b> (не удалось превью)\n{file_id}", reply_markup=kb)

# Шаг 1 Даблчека: Запрос подтверждения
@dp.callback_query(F.data.startswith("askpost:"))
async def ask_confirm_post(callback: CallbackQuery):
    if not check_access(callback.from_user.id): return
    idx = int(callback.data.split(":")[1])

    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, постим прямо сейчас!", callback_data=f"confirmpost:{idx}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"cancelpost:{idx}")
        ]
    ])

    await callback.message.edit_caption(
        caption=callback.message.caption + "\n\n⚠️ <b>Вы уверены, что хотите опубликовать этот арт ВНЕ очереди прямо сейчас?</b>",
        reply_markup=confirm_kb
    )
    await callback.answer()

# Шаг 2 Даблчека: Финальное подтверждение
@dp.callback_query(F.data.startswith("confirmpost:"))
async def confirm_post_now(callback: CallbackQuery):
    if not check_access(callback.from_user.id): return
    idx = int(callback.data.split(":")[1])

    queue = await load_queue_db()
    if 0 <= idx < len(queue):
        item = queue.pop(idx)
        await save_queue_db(queue)
        
        # Заглушка отправки (при слиянии подключится к CHANNEL_ID)
        await callback.message.edit_caption(
            caption=callback.message.caption + "\n\n🚀 <b>ПОСТ УСПЕШНО ОПУБЛИКОВАН В КАНАЛ!</b>"
        )
        await callback.answer("Опубликовано!")
    else:
        await callback.answer("❌ Пост не найден в очереди.", show_alert=True)

@dp.callback_query(F.data.startswith("cancelpost:"))
async def cancel_post_now(callback: CallbackQuery):
    if not check_access(callback.from_user.id): return
    idx = int(callback.data.split(":")[1])

    original_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🚀 Запостить сейчас", callback_data=f"askpost:{idx}"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delq:{idx}")
        ]
    ])

    clean_caption = callback.message.caption.split("\n\n⚠️")[0]
    await callback.message.edit_caption(caption=clean_caption, reply_markup=original_kb)
    await callback.answer("Отменено.")

@dp.callback_query(F.data.startswith("delq:"))
async def delete_from_queue(callback: CallbackQuery):
    if not check_access(callback.from_user.id): return
    idx = int(callback.data.split(":")[1])

    queue = await load_queue_db()
    if 0 <= idx < len(queue):
        queue.pop(idx)
        await save_queue_db(queue)
        await callback.message.delete()
        await callback.answer("Удалено из очереди!")

# --- ТЕКСТОВЫЕ КОМАНДЫ УПРАВЛЕНИЯ ---

@dp.message(F.text)
async def handle_commands(message: Message):
    if not check_access(message.from_user.id): return
    text = message.text.strip()
    global MODERATION_CHAT_ID

    if text == "/setmodgroup":
        MODERATION_CHAT_ID = message.chat.id
        await save_state()
        await message.reply(f"✅ Эта группа привязана как **Группа Модерации**!\nID: <code>{MODERATION_CHAT_ID}</code>")

    elif text.startswith("/addlabel"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            await message.reply("⚙️ <b>Формат:</b>\n<code>/addlabel Имя | теги | источники | эмодзи | режим | подпись</code>")
            return
        try:
            raw = parts[1].split("|")
            lname, ltags, lsrc, lemoji, lmode = [r.strip() for r in raw[:5]]
            lsig = raw[5].strip() if len(raw) > 5 else None
            sources = ["rule34", "gelbooru"] if lsrc.lower() == "all" else [lsrc.lower()]
            
            LABELS[:] = [lbl for lbl in LABELS if lbl["name"].lower() != lname.lower()]
            LABELS.append({"name": lname, "tags": ltags, "sources": sources, "emoji": lemoji, "mode": lmode.upper(), "signature": lsig})
            await save_state()
            await message.reply(f"✅ Лейбл <b>{lname}</b> успешно сохранен!")
        except Exception as e:
            await message.reply(f"❌ Ошибка формата: {e}")

    elif text == "/labels":
        if not LABELS:
            await message.reply("📭 Лейблов нет.")
            return
        txt = "🏷 <b>Активные лейблы:</b>\n\n"
        for i, l in enumerate(LABELS, 1):
            txt += f"{i}. {l.get('emoji', '🏷')} <b>{l['name']}</b> ({l.get('mode')})\n• Теги: <code>{l['tags']}</code>\n\n"
        await message.reply(txt)

    elif text.startswith("/checkpost"):
        parts = text.split(maxsplit=1)
        search_tag = parts[1].strip() if len(parts) > 1 else "femboy"
        target = MODERATION_CHAT_ID if MODERATION_CHAT_ID else message.chat.id

        await message.reply(f"🔍 Запрос к Rule34/Gelbooru по тегу <code>{search_tag}</code>...")
        posts = await fetch_booru_posts("rule34", search_tag, limit=5)
        if not posts: posts = await fetch_booru_posts("gelbooru", search_tag, limit=5)

        if not posts:
            await message.reply("❌ Постов не найдено.")
            return

        p = posts[0]
        caption = f"🧪 <b>Проверочный пост</b>\n🆔 ID: <code>{p.get('id')}</code>\n🔗 {p.get('file_url')}"
        await bot.send_photo(chat_id=target, photo=p.get('file_url'), caption=caption)

# --- СТАРТ ---

async def main():
    await init_db()
    await load_state()

    await bot.set_my_commands([
        BotCommand(command="queue", description="📊 Очередь и мгновенный постинг (Даблчек)"),
        BotCommand(command="labels", description="🏷 Посмотреть активные лейблы"),
        BotCommand(command="addlabel", description="➕ Добавить лейбл сбора"),
        BotCommand(command="checkpost", description="🔍 Прислать 1-й арт по тегу"),
        BotCommand(command="setmodgroup", description="📌 Привязать эту группу для модерации"),
    ])

    asyncio.create_task(parser_loop())
    logger.info("🤖 Бот-агрегатор успешно запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Остановлен пользователем")
