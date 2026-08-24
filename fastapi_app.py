# fastapi_app.py
import os
import asyncio
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response, status
import asyncpg

# Импортируем ваши модули валидации и ИИ
# Подразумевается, что они у вас написаны асинхронно или обернуты в asyncio.to_thread
# from validators import run_validation_level_2, run_validation_level_3

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "GlDxzFUy6V")
DB_HOST = os.getenv("DB_HOST", "82.146.18.156")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stroy_net")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

db_pool = None
listener_task = None  # Ссылка на фоновую задачу лисенера


async def handle_new_message(connection, pid, channel, payload):
    """
    🔥 Колбэк-функция, которая срабатывает МГНОВЕННО, когда в базу падает NOTIFY.
    payload — это переданный из триггера log_id.
    """
    log_id = payload
    print(f"🔔 [Поток ИИ] Перехвачен сигнал NOTIFY! Новое сообщение ID: {log_id}")

    # Запускаем асинхронную цепочку обработки (Уровни 2, 3 и т.д.)
    # Чтобы не блокировать лисенер, пускаем обработку фоном внутри asyncio
    asyncio.create_task(process_pipeline(int(log_id)))


async def process_pipeline(log_id: int):
    """Главный конвейер ИИ: запускает валидацию, авторизацию и вызов Ollama."""
    try:
        print(f"⚙️ [Конвейер] Начинаем обработку сообщения #{log_id}...")

        # Тут будет ваш код Уровня 2 (Проверка прав в employee_phones)
        # ...

        # Тут будет ваш код Уровня 3 (Разделение на задачи и болтовню через Regex + Ollama)
        # ...

        print(f"✅ [Конвейер] Сообщение #{log_id} успешно прошло обработку!")
    except Exception as e:
        print(f"❌ [Конвейер] Ошибка обработки сообщения #{log_id}: {e}")


async def pg_listener():
    """
    Бесконечный цикл лисенера. Выделяет отдельное постоянное соединение с БД,
    подписывается на канал и восстанавливает связь при сбоях.
    """
    while True:
        try:
            print("🎙️ Попытка подключения выделенного соединения для LISTEN...")
            conn = await asyncpg.connect(DATABASE_URL)

            # Регистрируем слушатель на ваш канал триггера
            await conn.add_listener('new_message_trigger', handle_new_message)
            print("🚀 Лисенер успешно подписался на канал 'new_message_trigger' и ждет данных...")

            # Держим соединение активным, пока работает приложение
            while True:
                await asyncio.sleep(5)
                # Проверяем, жива ли сеть легким пингом
                await conn.execute("SELECT 1;")

        except asyncio.CancelledError:
            print("🛑 Фоновая задача лисенера принудительно остановлена.")
            if 'conn' in locals():
                await conn.close()
            break
        except Exception as e:
            print(f"💥 Ошибка лисенера БД: {e}. Повторное подключение через 5 секунд...")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управляет пулом и фоновыми задачами при старте и выключении веб-сервера."""
    global db_pool, listener_task
    try:
        # 1. Запуск основного пула для HTTP-эндпоинтов
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
        print("⚡ Асинхронный пул соединений с PostgreSQL запущен.")

        # 2. Запуск фоновой задачи LISTEN в событийно-ориентированном цикле (Event Loop)
        listener_task = asyncio.create_task(pg_listener())

        yield
    finally:
        # Корректно завершаем задачи при остановке сервера
        if listener_task:
            listener_task.cancel()
            await asyncio.gather(listener_task, return_exceptions=True)
        if db_pool:
            await db_pool.close()
        print("🛑 Все службы и пулы StroyNet остановлены.")


app = FastAPI(title="StroyNet API Gateway", lifespan=lifespan)


@app.post("/webhook/max")
async def receive_max_webhook(request: Request):
    """Принимает вебхук от МАКС с жесткой фильтрацией групповых чатов."""
    try:
        payload = await request.json()

        # 1. Извлекаем объект сообщения и чата
        message_obj = payload.get("message", {})
        chat_obj = message_obj.get("chat", {})

        # 2. ПРОВЕРКА ТИПА ЧАТА: игнорируем всё, кроме личной переписки (private)
        # Если МАКС присылает плоскую структуру, проверяем ключ chat_type из корня
        chat_type = chat_obj.get("type") or payload.get("chat_type", "private")

        if chat_type in ["group", "supergroup", "channel"]:
            print(f"🚫 [Фильтр чатов] Игнорируем сообщение из группы/канала (Тип: {chat_type}).")
            # Возвращаем 200 OK, чтобы МАКС зафиксировал успешную доставку и не слал повторов
            return {"status": "ignored", "reason": "group_chats_not_allowed"}

        # 3. Извлекаем стандартные поля
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

        # 4. Уровень 1: Запись в БД, только если это личный чат
        query = """
            INSERT INTO message_logs (messenger_uid, text, validation_level, is_valid, intent_type)
            VALUES ($1, $2, 1, FALSE, 'unprocessed')
            RETURNING log_id;
        """
        async with db_pool.acquire() as connection:
            async with connection.transaction():
                log_id = await connection.fetchval(query, messenger_uid, message_text)
                await connection.execute(f"NOTIFY new_message_trigger, '{log_id}';")

        print(f"📥 [Уровень 1] Личное сообщение #{log_id} сохранено. Триггер отправлен.")
        return {"status": "success", "log_id": log_id}

    except Exception as e:
        print(f"❌ Ошибка эндпоинта вебхука: {e}")
        return Response(content="Internal Error", status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

