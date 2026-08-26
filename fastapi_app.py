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
async def generate_smart_response(text_msg: str, val_res: dict) -> str:
    """
    Генерирует умный ответ на основе типа сообщения, определенного валидатором.
    """
    intent_type = val_res.get("intent_type", "unknown")

    # 1. Если локальный валидатор понял, что это просто разговор (болталка)
    if intent_type == "chitchat":
        print("💬 Режим: Болталка. Просим Ollama ответить вежливо и поддержать разговор.")
        # Запускаем Ollama в обычном режиме чата
        return await parse_with_ollama(text_msg, mode="chat")

    # 2. Если это конкретная строительная задача (заказ бетона, арматуры, оси и т.д.)
    elif intent_type == "construction_task":
        print("🏗️ Режим: Строительная задача. Формируем строгий лаконичный промпт.")

        # Передаем контекст прямо в промпт, чтобы модель вела себя как четкий диспетчер
        strict_prompt = (
            "Ты — лаконичный диспетчер строительной логистики. Твоя задача — подтвердить приём заявки. "
            "Отвечай строго одной короткой фразой (не более 7 слов). Без лишних приветствий. "
            "Пример: 'Принято в обработку: 5 кубов бетона на 3 секцию.'"
        )
        # Отправляем в Ollama (если ваш клиент поддерживает кастомный промпт),
        # либо модель вернет ответ на основе текста.
        return await parse_with_ollama(f"{strict_prompt}\n\nСообщение прораба: {text_msg}")

    # 3. Если валидатор вообще ничего не понял (неизвестный тип)
    else:
        print("❓ Режим: Неизвестный контекст. Просим модель саму разобраться и ответить.")
        return await parse_with_ollama(text_msg, mode="chat")


# Функция, которая моментально сработает при появлении строки в message_logs
import json


async def process_new_message(payload_id: str):
    """Боевой конвейер ИИ: одинаково успешно понимает и чистые ID, и JSON-пакеты"""
    payload_str = payload_id.strip()
    log_id = None

    # 🚨 УНИВЕРСАЛЬНЫЙ РАЗБОР СИГНАЛА:
    try:
        if payload_str.startswith("{"):
            # Если база прислала сигнал в формате JSON, бережно распаковываем его
            data = json.loads(payload_str)
            log_id = int(data.get("log_id"))
            print(f"🧩 [СИГНАЛ БД] Распакован JSON. Успешно извлечен log_id: {log_id}")
        else:
            # Если база прислала сигнал в виде чистого числа-строки
            log_id = int(payload_str)
            print(f"🎯 [СИГНАЛ БД] Получен чистый числовой log_id: {log_id}")
    except Exception as e:
        # Если прилетел невалидный текст, который нельзя распарсить, выходим
        print(f"🗑️ [ФИЛЬТР] Пропущен некорректный сигнал базы: {e}")
        return

    if not log_id:
        return

    # --- ДАЛЬШЕ ВАШ НАДЕЖНЫЙ КОД БЕЗ ИЗМЕНЕНИЙ ---
    conn = None
    try:
        print(f"🚀 [КОНВЕЙЕР ИИ] Начинаем обработку log_id: {log_id}")
        conn = await asyncpg.connect(dsn=DATABASE_URL)

        row = await conn.fetchrow("""
            SELECT log_id, platform, chat_id, chat_type, messenger_uid, text, intent_type 
            FROM message_logs WHERE log_id = $1;
        """, log_id)

        if not row:
            return

        uid = row['messenger_uid']
        text_msg = row['text']
        intent = row['intent_type']

        if text_msg and (intent == 'unknown' or intent is None):
            val_res = fast_surface_validate(text_msg)
            ai_reply = await generate_smart_response(text_msg, val_res)

            # Закрываем входящую задачу (Слой 2)
            await conn.execute("""
                UPDATE message_logs SET intent_type = $1, validation_level = 2, is_valid = TRUE WHERE log_id = $2;
            """, val_res["intent_type"], log_id)

            # Ставим ОДНУ чистую задачу на отправку
            await conn.execute("""
                INSERT INTO outbound_messages (platform, chat_id, messenger_uid, text, status) 
                VALUES ($1, $2, $3, $4, 'pending');
            """, row['platform'] if row['platform'] else 'max_platform',
                               row['chat_id'] if row['chat_id'] else 'test_chat_777', uid, ai_reply.strip())
            print(f"🎉 [КОНВЕЙЕР ИИ] Ответ для log_id {log_id} успешно отправлен в outbound_messages!")

    except Exception as e:
        print(f"❌ Ошибка конвейера ИИ для ID {log_id}: {e}")
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
# =====================================================================
# ЖИЗНЕННЫЙ ЦИКЛ FASTAPI (LIFESPAN)
# =====================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Слушатель теперь надёжно крутится в main_fastapi.py,
    # поэтому здесь мы просто пишем чистый лог старта
    print("🚀 FastAPI Lifespan: Сетевой шлюз StroyNet запущен!")

    yield

    # При выключении сервера просто пишем лог, без вызова отменённых задач
    print("🛑 FastAPI Lifespan: Сетевой шлюз успешно остановлен.")


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