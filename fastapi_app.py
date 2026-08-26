# fastapi_app.py
import os
import asyncio
import asyncpg
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
from config import DATABASE_URL
from ollama_client import parse_with_ollama
from validators import fast_surface_validate


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
        # Парсим JSON-строку, которую передаёт триггер
        import json
        data = json.loads(payload)
        log_id = data.get('log_id')

        if not log_id:
            print(f"⚠️ [FastAPI] В payload нет log_id: {payload}")
            return

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
            print(f"✅ [FastAPI] Сообщение {log_id} обработано")

    except Exception as e:
        print(f"❌ [FastAPI] Ошибка обработки сообщения: {e}")


async def listen_for_messages():
    print("🔥 listen_for_messages() вызвана")  # <-- добавить
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

# Функция, которая моментально сработает при появлении строки в message_logs
async def process_new_message(payload_id: str):
    """Прямой транспорт сообщений: БД -> Валидатор -> Ollama -> БД"""
    print(f"📥 Конвейер: Получен сигнал для log_id {payload_id}")

    conn = None
    try:
        log_id = int(payload_id)
        conn = await asyncpg.connect(dsn=DATABASE_URL)

        # 1. Стягиваем текст сообщения
        row = await conn.fetchrow("""
            SELECT log_id, text, intent_type 
            FROM message_logs 
            WHERE log_id = $1;
        """, log_id)

        if not row:
            return

        text_msg = row['text']
        intent = row['intent_type']

        # Обрабатываем только новые необработанные сообщения
        if text_msg and (intent == 'unknown' or intent is None):

            # ШАГ 2. ПЕРВИЧНАЯ ВАЛИДАЦИЯ (Словарь стоп-слов)
            # Передаем текст в ваш валидатор. Если это мусор/тест — стопаем конвейер
            if not fast_surface_validate(text_msg):
                print(f"🚫 Сообщение {log_id} не прошло первичный валидатор (мусор/тест).")
                await conn.execute("""
                    UPDATE message_logs 
                    SET intent_type = 'skipped_by_validator', is_valid = FALSE 
                    WHERE log_id = $1;
                """, log_id)
                return

            # ШАГ 3. ТРАНСПОРТ ДО OLLAMA
            print(f"🚀 Валидация успешна. Отправляем текст в Ollama...")

            # Передаем текст в вашу существующую функцию (она вернет живой ответ)
            ai_reply = await parse_with_ollama(text_msg)

            # ШАГ 4. ЗАПИСЬ ОТВЕТА В БД
            # Обновляем интент ответом от ИИ и переводим на следующий слой
            await conn.execute("""
                UPDATE message_logs 
                SET intent_type = $1, 
                    validation_level = 2, 
                    is_valid = TRUE 
                WHERE log_id = $2;
            """, ai_reply[:50], log_id)  # Ограничим до 50 символов для безопасности поля

            print(f"🎉 Конвейер завершен! Ответ ИИ для {log_id} записан в базу.")

    except Exception as e:
        print(f"❌ Ошибка транспорта сообщений: {e}")
    finally:
        if conn:
            await conn.close()


async def db_notification_listener():
    """Фоновый процесс, который держит постоянное соединение и слушает триггер"""
    loop = asyncio.get_running_loop()

    # Внутренний обработчик, который вызывается при получении сигнала из БД
    def handle_notification(connection, pid, channel, payload):
        # Потокобезопасно планируем выполнение асинхронной функции обработки
        loop.call_soon_threadsafe(asyncio.create_task, process_new_message(payload))

    while True:
        try:
            # Открываем отдельное подключение для прослушивания канала
            conn = await asyncpg.connect(dsn=DATABASE_URL)
            print("📡 Фоновый слушатель БД успешно подписался на канал 'new_message_event'")

            # Регистрируем надежный именованный обработчик событий
            await conn.add_listener('new_message_event', handle_notification)

            # Держим соединение активным
            while True:
                await asyncio.sleep(3600)

        except (asyncpg.PostgresError, OSError) as e:
            print(f"⚠️ Ошибка слушателя БД ({e}). Переподключение через 5 секунд...")
            await asyncio.sleep(5)



# Интегрируем в жизненный цикл FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Запускаем фонового слушателя параллельно с основным сервером FastAPI
    listener_task = asyncio.create_task(db_notification_listener())
    yield
    # При остановке FastAPI корректно завершаем фоновую задачу
    listener_task.cancel()
    try:
        await listener_task
    except asyncio.CancelledError:
        print("🛑 Фоновый слушатель БД остановлен.")


# Регистрируем lifespan в вашем FastAPI приложении
app = FastAPI(lifespan=lifespan)


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