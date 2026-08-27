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
from database import db


from dotenv import load_dotenv

load_dotenv()
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
    print("🔥 listen_for_messages() вызвана")

    # Объявляем переменную. Теперь она будет активно использоваться внутри цикла
    conn = None

    while True:
        try:
            # 1. Открываем соединение, используя глобальный DATABASE_URL
            conn = await asyncpg.connect(dsn=DATABASE_URL)
            print("📡 Фоновый слушатель БД успешно подписался на канал 'new_message_event'")

            # 2. ИСПОЛЬЗУЕМ conn! Вешаем обработчик событий базы данных
            # (Здесь conn больше НЕ БУДЕТ серой, так как мы вызываем её метод add_listener)
            await conn.add_listener('new_message_event', lambda connection, pid, channel, payload:
            asyncio.create_task(process_new_message(payload))
                                    )

            # 3. Удерживаем это конкретное соединение открытым
            while True:
                await asyncio.sleep(3600)

        except (asyncpg.PostgresError, OSError) as e:
            print(f"⚠️ Ошибка слушателя БД ({e}). Переподключение через 5 секунд...")
            await asyncio.sleep(5)


# =====================================================================
# LIFECYCLE
# =====================================================================
# Внутри fastapi_app.py

async def generate_smart_response(text_msg: str, val_res: dict) -> str:
    """Генерирует умный простой ответ на основе типа сообщения (СТАРТОВОЕ СОСТОЯНИЕ)."""
    intent_type = val_res.get("intent_type", "unknown")

    # 1. Если локальный валидатор определил болталку
    if intent_type == "chitchat":
        print(" [FastAPI] Режим: Болталка. Ollama отвечает вежливо.")
        return await parse_with_ollama(text_msg, mode="chat")

    # 2. Если это конкретная строительная задача (заказ бетона, арматуры и т.д.)
    elif intent_type == "construction_task":
        print(" [FastAPI] Режим: Строительная задача. Применяем базовый промпт на 20 слов.")

        # Наш стартовый автономный промпт диспетчера
        strict_prompt = (
            "Ты — диспетчер строительной логистики. Твоя задача — подтвердить приём заявки. "
            "Отвечай одной фразой (не более 20 слов)."
        )

        # Склеиваем промпт и сообщение прораба
        full_prompt = f"{strict_prompt}\n\nСообщение прораба: {text_msg}\nОтвет диспетчера:"
        return await parse_with_ollama(full_prompt)

    # 3. Неизвестный контекст
    else:
        print(" [FastAPI] Режим: Неизвестный контекст. Автономный ответ.")
        return await parse_with_ollama(text_msg, mode="chat")


async def process_new_message(payload_id: str):
    """Основной конвейер обработки сообщения."""
    payload_str = payload_id.strip()
    log_id = None
    try:
        if payload_str.startswith("{"):
            data = json.loads(payload_str)
            log_id = int(data.get("log_id"))
        else:
            log_id = int(payload_str)
    except Exception as e:
        print(f" [ФИЛЬТР] Ошибка парсинга: {e}")
        return

    if not log_id:
        return

    conn = None
    try:
        print(f"\n🔹 [ШАГ 1] Подключаюсь к БД для ID {log_id}")
        conn = await asyncpg.connect(dsn=DATABASE_URL)

        row = await conn.fetchrow("""
            SELECT log_id, platform, chat_id, chat_type, messenger_uid, text, intent_type
            FROM message_logs WHERE log_id = $1;
        """, log_id)

        if not row:
            print(f" [ШАГ 1] Строка {log_id} не найдена в базе. conn - {conn}!")
            return

        messenger_uid = row['messenger_uid']

        text_msg = row['text']
        print(f"🔹 [ШАГ 2] Запускаю валидатор для текста: '{text_msg}'")
        val_res = fast_surface_validate(text_msg)

        print(f"🔹 [ШАГ 3] Отправляю запрос в Ollama...")

        # 🚀 ЖЕЛЕЗОБЕТОННЫЙ ВЫЗОВ: Передаем ровно два аргумента по порядку, как просит Python
        db_context = await db.get_full_user_context(row['messenger_uid'], conn)

        print(f"📡 [ШАГ 3 ТЕСТ КОНТЕКСТА]: {db_context}")

        print(f"🔹 [ШАГ 4] Данные Many-to-Many успешно получены.")

        # 🚀 ИСПРАВЛЕНИЕ: Передаем наше живое соединение conn в параметр pool по имени!
        db_context = await db.get_full_user_context(row['messenger_uid'], pool=conn)

        print(f"📡 [ШАГ 3 ТЕСТ КОНТЕКСТА]: {db_context}")

        print(f"🔹 [ШАГ 4] Данные Many-to-Many успешно получены.")

        # Шаг 5: Запись в outbound_messages
        await conn.execute("""
                    INSERT INTO outbound_messages (platform, chat_id, messenger_uid, text, status)
                    VALUES ($1, $2, $3, $4, 'pending');
                """, row['platform'] or 'max_platform', row['chat_id'] or 'test_chat_777', row['messenger_uid'],
                           str(db_context))

        print(f"🔹 [ШАГ 5] Успешно записано в outbound_messages!")

    except Exception as e:
        print(f"❌ [СБРОС] Ошибка в процессе обработки ID {log_id}: {e}")
    finally:
        if conn:
            await conn.close()
            print("🔗 Соединение conn успешно закрыто.")


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
# =====================================================================
# ЖИЗНЕННЫЙ ЦИКЛ FASTAPI (LIFESPAN)
# =====================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    # Там, где у вас создается пул (например, db_pool = await asyncpg.create_pool...)
    print(" FastAPI Lifespan: Сетевой шлюз StroyNet запущен!")

    # 🚀 СИНХРОНИЗИРУЕМ ПУЛЫ ОДИН РАЗ ПРИ СТАРТЕ:
    db.pool = db_pool
    yield
    print(" FastAPI Lifespan: Сетевой шлюз успешно остановлен.")


# Оставляем ОДНО красивое объявление приложения с заголовком
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