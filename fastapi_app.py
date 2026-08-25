# fastapi_app.py
import os
import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
import asyncpg
from dotenv import load_dotenv

load_dotenv()

# Импортируем ваши модули валидации и ИИ
# from validators import run_validation_level_2, run_validation_level_3

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stroy_net")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

db_pool = None
listener_task = None


# =====================================================================
# СЛУШАТЕЛЬ УВЕДОМЛЕНИЙ ИЗ БД
# =====================================================================

async def handle_new_message(connection, pid, channel, payload):
    """Обрабатывает уведомление о новом сообщении в БД."""
    try:
        log_id = int(payload)
        print(f"🔔 [FastAPI] Новое сообщение в БД: ID {log_id}")

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT messenger_uid, text, source_type, file_path FROM message_logs WHERE log_id = $1",
                log_id
            )
            if not row:
                print(f"⚠️ Сообщение {log_id} не найдено в БД")
                return

            messenger_uid = row['messenger_uid']
            text = row['text']
            source_type = row.get('source_type', 'text')
            file_path = row.get('file_path')

            print(f"📩 [FastAPI] Обработка сообщения от {messenger_uid}: {text[:50]}...")

            # --- ТУТ БУДЕТ ЛОГИКА ОБРАБОТКИ ---
            # Если source_type == 'voice' → вызываем Whisper
            # Если source_type == 'photo' → вызываем YOLO + OCR
            # Если source_type == 'text' → отправляем в Ollama

            # Пока просто логируем
            print(f"✅ [FastAPI] Сообщение {log_id} обработано")

    except Exception as e:
        print(f"❌ [FastAPI] Ошибка обработки сообщения {log_id}: {e}")


async def listen_for_messages():
    """Подключается к БД и слушает уведомления."""
    conn = None
    try:
        conn = await asyncpg.connect(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME
        )
        await conn.add_listener('new_message', handle_new_message)
        print("🔔 FastAPI слушает уведомления из БД")
        while True:
            await asyncio.sleep(1)
    except Exception as e:
        print(f"❌ Ошибка слушателя БД: {e}")
    finally:
        if conn:
            await conn.close()


# =====================================================================
# LIFECYCLE
# =====================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, listener_task
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
        print("⚡ Пул FastAPI запущен. Только для обслуживания ручек.")

        # Запускаем слушатель уведомлений
        listener_task = asyncio.create_task(listen_for_messages())
        print("🔔 Слушатель уведомлений запущен")

        yield
    finally:
        if listener_task:
            listener_task.cancel()
        if db_pool:
            await db_pool.close()


app = FastAPI(title="StroyNet API Gateway", lifespan=lifespan)


# =====================================================================
# ЭНДПОИНТЫ
# =====================================================================

@app.post("/webhook/max")
async def receive_max_webhook(request: Request):
    """Принимает вебхук от МАКС с жесткой фильтрацией групповых чатов."""
    try:
        payload = await request.json()

        message_obj = payload.get("message", {})
        chat_obj = message_obj.get("chat", {})

        chat_type = chat_obj.get("type") or payload.get("chat_type", "private")

        if chat_type in ["group", "supergroup", "channel"]:
            print(f"🚫 [Фильтр чатов] Игнорируем сообщение из группы/канала (Тип: {chat_type}).")
            return {"status": "ignored", "reason": "group_chats_not_allowed"}

        if "from" in message_obj:
            messenger_uid = str(message_obj["from"].get("id", ""))
        else:
            messenger_uid = str(payload.get("user_id") or payload.get("messenger_uid", ""))

        message_text = ""
        if "text" in message_obj:
            message_text = str(message_obj.get("text", "")).strip()
        else:
            message_text = str(payload.get("text") or payload.get("message", "")).strip()

        if not messenger_uid or not message_text:
            return Response(content="Bad Request: Missing UID or Text", status_code=status.HTTP_400_BAD_REQUEST)

        query = """
            INSERT INTO message_logs (messenger_uid, text, validation_level, is_valid, intent_type)
            VALUES ($1, $2, 1, FALSE, 'unprocessed')
            RETURNING log_id;
        """
        async with db_pool.acquire() as connection:
            async with connection.transaction():
                log_id = await connection.fetchval(query, messenger_uid, message_text)
                await connection.execute(f"NOTIFY new_message, '{log_id}';")

        print(f"📥 [Уровень 1] Личное сообщение #{log_id} сохранено. Триггер отправлен.")
        return {"status": "success", "log_id": log_id}

    except Exception as e:
        print(f"❌ Ошибка эндпоинта вебхука: {e}")
        return Response(content="Internal Error", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@app.get("/ping")
async def ping():
    return {"status": "alive", "service": "stroy-net-dispatcher"}