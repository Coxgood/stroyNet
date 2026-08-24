# filename: listener_v001.py
# description: слушает MAX, регистрирует сотрудников, сохраняет сырые сообщения в message_logs
# depends: config.py, database.py
# runs_as: демон (фоновый процесс)

import asyncio
import aiohttp
import asyncpg
import json
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, MAX_TOKEN, MAX_BASE_URL

BASE_URL = MAX_BASE_URL

# =====================================================================
# ФУНКЦИЯ: регистрация сотрудника (без дублей)
# =====================================================================
async def ensure_employee_exists(pool, user_id, first_name, last_name):
    async with pool.acquire() as conn:
        # Проверяем по messenger_uid
        check_acc = "SELECT employee_id FROM employee_accounts WHERE messenger_uid = $1 AND platform = 'max';"
        employee_id = await conn.fetchval(check_acc, user_id)
        if employee_id:
            return employee_id

        # Проверяем по phone
        phone = f"max_{user_id}"
        check_emp = "SELECT employee_id FROM employees WHERE phone = $1;"
        employee_id = await conn.fetchval(check_emp, phone)
        if employee_id:
            await conn.execute(
                "INSERT INTO employee_accounts (employee_id, platform, messenger_uid) VALUES ($1, 'max', $2);",
                employee_id, user_id
            )
            return employee_id

        # Создаём нового
        print(f"🆕 Новый сотрудник: {first_name} {last_name} (ID: {user_id})")
        async with conn.transaction():
            insert_emp = """
                INSERT INTO employees (first_name, last_name, phone, telegram_uid)
                VALUES ($1, $2, $3, 'не указан')
                RETURNING employee_id;
            """
            new_id = await conn.fetchval(insert_emp, first_name, last_name, phone)
            await conn.execute(
                "INSERT INTO employee_accounts (employee_id, platform, messenger_uid) VALUES ($1, 'max', $2);",
                new_id, user_id
            )
            return new_id


# =====================================================================
# ФУНКЦИЯ: сохранение сообщения в message_logs
# =====================================================================
async def save_inbound_log(pool, chat_id, messenger_uid, text,
                           intent_type='transaction', confidence=80, priority=5,
                           validation_level=1, validation_score=80,
                           source_type='text', access_level=1):
    query = """
        INSERT INTO message_logs (
            platform, chat_id, chat_type, messenger_uid, direction, text,
            intent_type, confidence_score, priority,
            validation_level, validation_score, source_type, access_level
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);
    """
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                query,
                'max', chat_id, 'private', messenger_uid, 'inbound', text,
                intent_type, confidence, priority,
                validation_level, validation_score, source_type, access_level
            )
            print(f"💾 Сохранено: {text[:50]}...")
    except Exception as e:
        print(f"❌ Ошибка записи: {e}")


# =====================================================================
# ФУНКЦИЯ: обработка пачки событий от MAX
# =====================================================================
async def process_updates(updates, pool):
    if not isinstance(updates, dict):
        return None

    next_marker = updates.get("marker")
    events = updates.get("updates", [])

    for event in events:
        if event.get("update_type") == "message_created" and event.get("message"):
            msg = event["message"]
            user_id = str(msg.get("sender", {}).get("user_id", ""))
            first_name = msg.get("sender", {}).get("first_name", "Строитель")
            last_name = msg.get("sender", {}).get("last_name", "Новый")
            chat_id = str(msg.get("recipient", {}).get("chat_id", ""))

            employee_id = await ensure_employee_exists(pool, user_id, first_name, last_name)

            body = msg.get("body", {})
            text = body.get("text", "")
            media = body.get("media", {})
            media_type = media.get("type") if media else None

            if text and not media_type:
                await save_inbound_log(
                    pool, chat_id, user_id, text,
                    intent_type='transaction',
                    confidence=80,
                    priority=5,
                    validation_level=1,
                    validation_score=80,
                    source_type='text',
                    access_level=1
                )
            else:
                print(f"⏭️ Игнор: {media_type or 'неизвестный тип'}")

    return next_marker


# =====================================================================
# ГЛАВНЫЙ ЦИКЛ
# =====================================================================
async def main():
    print("🤖 Listener v0.001 запущен. Подключение к БД...")

    if not MAX_TOKEN:
        print("❌ Токен не найден!")
        return

    try:
        pool = await asyncpg.create_pool(
            user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME,
            min_size=1, max_size=10
        )
        print("✅ База на связи.")
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
        return

    headers = {"Authorization": MAX_TOKEN, "Content-Type": "application/json"}
    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        marker = None
        print("🚀 Слушаю MAX...")

        while True:
            try:
                params = {"timeout": 30}
                if marker:
                    params["marker"] = marker

                async with session.get(f"{BASE_URL}/updates", params=params) as response:
                    if response.status == 200:
                        updates = await response.json()
                        new_marker = await process_updates(updates, pool)
                        if new_marker:
                            marker = new_marker
                    elif response.status == 404:
                        print("🔄 Маркер сброшен (404)")
                        marker = None
                        await asyncio.sleep(2)
                    elif response.status == 409:
                        print("⚠️ Конфликт сессий, жду 5 сек...")
                        await asyncio.sleep(5)
                    else:
                        print(f"⚠️ Статус: {response.status}")
                        await asyncio.sleep(2)
            except Exception as e:
                print(f"⚠️ Ошибка: {e}")
                await asyncio.sleep(3)

            await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлен.")