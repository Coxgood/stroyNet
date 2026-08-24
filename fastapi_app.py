# filename: fastapi_app.py
# description: основной API для приёма сообщений и парсинга через Ollama
import asyncio
import asyncpg
import aiohttp
import json
import logging
from datetime import datetime
from fastapi import FastAPI, status, HTTPException
from pydantic import BaseModel
from typing import Optional
from config import MAX_TOKEN, MAX_BASE_URL, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
from database import db
from ollama_client import parse_with_ollama
from validators import fast_surface_validate

# Используем системный логгер Uvicorn, чтобы логи гарантированно доходили до консоли
logger = logging.getLogger("uvicorn.error")

app = FastAPI(title="StroyNet Dispatcher", version="1.0")


class UniversalInputMessage(BaseModel):
    platform: str
    chat_id: str
    chat_type: str
    user_id: str
    sender_name: str
    text: str
    message_uid: Optional[str] = None


# Глобальный словарь лимитов
last_reply_time = {}


# ==============================================================================
# ПОДПРОГРАММЫ ДЛЯ ОТПРАВКИ И ОБРАБОТКИ (Объявлены в самом начале)
# ==============================================================================

async def send_to_max(chat_id: str, text: str):
    url = f"{MAX_BASE_URL}/messages"
    headers = {"Authorization": MAX_TOKEN}
    connector = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        await session.post(url, params={"chat_id": chat_id}, json={"text": text})


async def process_ollama_reply_task(payload: str):
    """Фоновый 3-уровневый конвейер обработки сообщений строителей."""
    try:
        data = json.loads(payload)

        # 1. Извлекаем переменные и проверяем тип чата
        chat_type = data.get('chat_type', 'private')
        chat_id = data.get('chat_id')
        messenger_uid = data.get('messenger_uid')
        text = data.get('text', '')

        if chat_type != 'private':
            logger.info(f"🤫 Пропускаем: групповой чат ({chat_type}).")
            return

        if data.get('direction') != 'inbound':
            return

        # === НАЧАЛО ЗАМЕНЫ ===

        # Находим ID записи в логах для смены уровней валидации
        log_id = data.get('log_id') or await db.get_last_log_id_for_user(messenger_uid)

        # Предохранитель: если лог вообще не найден в базе, останавливаемся, чтобы не упасть
        if not log_id:
            logger.warning(f"⚠️ [КОНВЕЙЕР] Не удалось найти log_id в базе для пользователя {messenger_uid}")
            return

        logger.info(f"🏁 [КОНВЕЙЕР] Старт лога #{log_id}. Уровень 1 пройден.")

        # ==============================================================================
        # 🛡️ УРОВЕНЬ 2: ПРОВЕРКА ДОПУСКА СОТРУДНИКА (Демо-режим)
        # ==============================================================================
        # Метод check_user_exists_by_uid в database.py теперь всегда возвращает True для демо
        is_employee = await db.check_user_exists_by_uid(messenger_uid)
        logger.info(f"✅ [КОНВЕЙЕР #2] Уровень 2 пройден (Авторизация: {is_employee}).")

        # ==============================================================================
        # 📊 УРОВЕНЬ 3: ПОВЕРХНОСТНАЯ ВАЛИДАЦИЯ И ОТВЕТ
        # ==============================================================================
        analysis = fast_surface_validate(text)

        # Записываем стадию 3 в PostgreSQL
        await db.update_validation_status(
            log_id=log_id,
            level=3,
            is_valid=analysis["is_valid"],
            score=analysis["confidence_score"],
            intent=analysis["intent_type"]
        )

        # Сценарий А: Полный мусор или непонятный текст
        if not analysis["is_valid"]:
            await send_to_max(chat_id,
                              "ℹ️ Не совсем понял строительный запрос. Напишите подробнее: какой материал, объем или технику нужно заказать?")
            return

        # Сценарий Б: Чистая болтовня или приветствие
        if analysis["intent_type"] == "chitchat":
            # Проверяем 2-минутный спам-лимит только для болтовни!
            if messenger_uid:
                now = datetime.now()
                if messenger_uid in last_reply_time:
                    elapsed = (now - last_reply_time[messenger_uid]).total_seconds()
                    if elapsed < 120:
                        logger.info(f"⏳ Пропускаем флуд {messenger_uid}: прошло {elapsed:.0f} сек.")
                        return

            # Вызываем Ollama в режиме простого текстового чата
            reply_text = await parse_with_ollama(text, mode="chat")
            await send_to_max(chat_id, reply_text)

            if messenger_uid:
                last_reply_time[messenger_uid] = datetime.now()
            return

        # Сценарий В: РЕАЛЬНАЯ СТРОИТЕЛЬНАЯ ЗАЯВКА (Идет без таймера, обрабатываем ВСЕГДА!)
        if analysis["intent_type"] == "construction_task":
            logger.info(f"🏗️ [КОНВЕЙЕР #3] Обнаружена строительная задача! Вызываем Ollama.")

            # На первом этапе демо возвращаем красивый текстовый ответ ИИ
            reply_text = await parse_with_ollama(text, mode="chat")

            # Отправляем прорабу подтверждение
            await send_to_max(chat_id, f"👷‍♂️ Терминатор-Диспетчер принял заявку:\n\n{reply_text}")
            return

    except Exception as e:
        logger.error(f"❌ Ошибка в фоновом таске конвейера: {e}", exc_info=True)



def handle_new_message(connection, pid, channel, payload):
    """Синхронный колбэк для asyncpg.

    Мгновенно перенаправляет сообщение в Event Loop и освобождает шину БД.
    """
    asyncio.create_task(process_ollama_reply_task(payload))


# ==============================================================================
# СЛУШАТЕЛЬ УВЕДОМЛЕНИЙ ИЗ БД
# ==============================================================================

async def listen_for_messages():
    """Слушает уведомления из БД и автоматически восстанавливает связь при сбоях."""
    logger.info("🔥 Слушатель запущен (фоновый процесс активирован)")

    while True:
        conn = None
        try:
            # Создаем новое подключение
            conn = await asyncpg.connect(
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME
            )

            # Регистрируем функцию обратного вызова (она теперь объявлена выше!)
            await conn.add_listener('new_message', handle_new_message)
            logger.info("🔔 FastAPI успешно подключился и слушает уведомления из БД")

            # Удерживаем соединение открытым (Heartbeat проверка)
            while True:
                await conn.execute("SELECT 1")
                await asyncio.sleep(30)

        except asyncio.CancelledError:
            logger.warning("🔇 Слушатель БД остановлен пользователем/системой")
            if conn:
                await conn.close()
            break  # Выходим из внешнего цикла при намеренной остановке

        except Exception as e:
            logger.error(f"❌ Ошибка соединения или слушателя БД: {e}", exc_info=True)
            logger.info("🔄 Попытка переподключения через 5 секунд...")
            if conn:
                try:
                    await conn.close()
                except Exception:
                    pass
            await asyncio.sleep(5)


# ==============================================================================
# LIFECYCLE (Жизненный цикл FastAPI с отловом ошибок старта)
# ==============================================================================

def handle_task_result(task: asyncio.Task):
    """Служебный колбэк: перехватывает фатальные ошибки падения таски при запуске."""
    try:
        task.result()
    except Exception as e:
        logger.critical(f"💥 ФАТАЛЬНЫЙ СБОЙ: Слушатель БД упал сразу после старта: {e}", exc_info=True)


@app.on_event("startup")
async def startup():
    global listener_task
    await db.connect()

    # Запускаем задачу слушателя
    listener_task = asyncio.create_task(listen_for_messages())

    # Вешаем предохранитель. Если таска упадет из-за синтаксиса, мы увидим трейсбэк
    listener_task.add_done_callback(handle_task_result)

    logger.info("🚀 FastAPI запущен, слушатель уведомлений активен")


@app.on_event("shutdown")
async def shutdown():
    global listener_task
    if listener_task:
        logger.info("⏳ Завершение работы: остановка слушателя БД...")
        listener_task.cancel()
        try:
            await listener_task
        except asyncio.CancelledError:
            pass
    await db.disconnect()
    logger.info("🛑 FastAPI остановлен")


# ==============================================================================
# ЭНДПОИНТЫ API
# ==============================================================================

@app.post("/api/v1/message", status_code=status.HTTP_200_OK)
async def handle_message(msg: UniversalInputMessage):
    try:
        # 1. ЛОГИРОВАНИЕ
        await db.log_incoming_message(
            platform=msg.platform,
            chat_id=msg.chat_id,
            chat_type=msg.chat_type,
            messenger_uid=msg.user_id,
            text=msg.text,
            message_uid=msg.message_uid,
            intent_type='transaction' if len(msg.text) > 5 else 'unknown',
            confidence_score=80 if len(msg.text) > 5 else 30,
            priority=5,
            validation_level=1,
            validation_score=80 if len(msg.text) > 5 else 30,
            source_type='text',
            access_level=1
        )
        logger.info(f"📋 [LOG] Сообщение от {msg.sender_name} сохранено в message_logs.")

        # 2. ПРОВЕРКА / РЕГИСТРАЦИЯ ПОЛЬЗОВАТЕЛЯ
        is_registered = await db.check_user_exists(msg.platform, msg.user_id)
        if not is_registered:
            await db.register_user(
                platform=msg.platform,
                user_id=msg.user_id,
                first_name=msg.sender_name
            )
            reply = (
                f"Приветствую, {msg.sender_name}! "
                f"Вы зафиксированы в системе. Для активации роли обратитесь к админу."
            )
            await send_to_max(msg.chat_id, reply)
            return {"status": "new_user_registered", "user_id": msg.user_id}

        # 3. ПАРСИНГ ЧЕРЕЗ OLLAMA (для заявок на эндпоинте)
        parsed = await parse_with_ollama(msg.text)
        logger.info(f"🤖 [OLLAMA] Распарсено на эндпоинте: {parsed}")

        if "error" in parsed:
            await db.save_unprocessed_order(
                messenger_uid=msg.user_id,
                raw_text=msg.text,
                error=parsed.get("error", "Неизвестная ошибка")
            )
            reply = f"Не удалось распарсить заявку. Проверьте формат сообщения."
            await send_to_max(msg.chat_id, reply)
            return {"status": "parsing_error", "parsed": parsed}

        # 4. СОХРАНЕНИЕ В ORDERS
        await db.save_parsed_order(
            messenger_uid=msg.user_id,
            raw_text=msg.text,
            parsed_data=parsed
        )
        logger.info(f"📦 [ЗАЯВКА] Сохранена в orders для {msg.sender_name}")

        # 5. ОТВЕТ В CHAT
        reply = f"Заявка принята! Распознано: {parsed}"
        await send_to_max(msg.chat_id, reply)
        return {"status": "success", "parsed": parsed}

    except Exception as e:
        logger.error(f"💥 [ОШИБКА ЭНДПОИНТА] {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ping")
async def ping():
    return {"status": "alive", "service": "stroy-net-dispatcher"}
