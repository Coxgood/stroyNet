# listener_v001.py (Эталонный рабочий фрагмент)
import os
import asyncio
import httpx
import asyncpg
import json

# Конфигурация собирается СТРОГО локально
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "GlDxzFUy6V")
DB_NAME = os.getenv("DB_NAME", "stroy_net")
DB_PORT = os.getenv("DB_PORT", "5432")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@127.0.0.1:{DB_PORT}/{DB_NAME}"

MAX_BASE_URL = os.getenv("MAX_BASE_URL", "https://platform-api2.max.ru")
MAX_TOKEN = os.getenv("MAX_TOKEN",
                      "f9LHodD0cOKvY0QG7ysxONTv-IGk5yxXNd_-7VCRjAqGI5SPN3KSnA9vMF5SKtVeN5ZuDafkyCqoSWUV4O6t")
HEADERS = {"Authorization": MAX_TOKEN}  # Без слова Bearer!


async def process_message(update_data, pool):
    """Безопасный разбор специфической структуры MAX API."""
    try:
        # Шаг 0: Жесткая проверка типа данных (лечим 'str' has no attribute 'get')
        if isinstance(update_data, str):
            update_data = json.loads(update_data)

        if not isinstance(update_data, dict):
            return

        # В МАКСе тело события может лежать внутри массива или объекта обновления
        message_obj = update_data.get("message", {})
        if not message_obj:
            message_obj = update_data  # Если структура плоская

        chat_obj = message_obj.get("chat", {})

        # 1. Защита от групп: проверяем тип чата
        if isinstance(chat_obj, dict) and chat_obj.get("type") in ["group", "supergroup"]:
            return

        text = message_obj.get("text", "").strip()

        # 2. Идентификация по MAX API: ищем 'sender' вместо 'from'
        sender_obj = message_obj.get("sender", {})
        messenger_uid = ""

        if isinstance(sender_obj, dict):
            # Извлекаем ID отправителя в системе МАКС
            messenger_uid = str(sender_obj.get("user_id") or sender_obj.get("id", ""))
        else:
            messenger_uid = str(message_obj.get("user_id") or update_data.get("messenger_uid", ""))

        if not text or not messenger_uid:
            return

        # --- НАЧАЛО ИИ-КОНВЕЙЕРА (Запись и авторизация) ---
        async with pool.acquire() as conn:
            # Уровень 1: Запись в лог
            query = """
                INSERT INTO message_logs (messenger_uid, text, validation_level, is_valid, intent_type)
                VALUES ($1, $2, 1, FALSE, 'unprocessed')
                RETURNING log_id;
            """
            log_id = await conn.fetchval(query, messenger_uid, text)
            print(f"📥 [Уровень 1] Сообщение #{log_id} сохранено в БД (UID: {messenger_uid}).")

            # Уровень 2: Авторизация прораба по БД
            has_access = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM employee_phones WHERE messenger_uid = $1);",
                messenger_uid
            )

            if not has_access:
                # Специально выводим UID в консоль, чтобы завтра скопировать его в таблицу!
                print(f"❌ [Уровень 2] Доступ ЗАПРЕЩЕН для UID: {messenger_uid}. Прораб не авторизован.")
                await conn.execute("""
                    UPDATE message_logs 
                    SET validation_level = 2, is_valid = FALSE, intent_type = 'unauthorized' 
                    WHERE log_id = $1;
                """, log_id)
                return

            print(f"✅ [Уровень 2] Доступ РАЗРЕШЕН для прораба UID: {messenger_uid}")
            await conn.execute("UPDATE message_logs SET validation_level = 2, is_valid = TRUE WHERE log_id = $1;",
                               log_id)

            # --- Завтра сюда прикручиваем Уровень 3 (Regex) и Уровень 4 (Ollama JSON) ---

    except Exception as e:
        print(f"💥 Ошибка обработки события: {e}")


async def main():
    print("🎙️ Запуск фонового лисенера StroyNet на Long Polling...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # Отключаем старые вебхуки, чтобы активировать Long Polling в МАКС
    async with httpx.AsyncClient(verify=False) as client:
        try:
            await client.post(f"{MAX_BASE_URL}/subscriptions", headers=HEADERS, json={"webhook_url": ""})
        except Exception:
            pass

        while True:
            try:
                # Опрашиваем МАКС
                response = await client.get(f"{MAX_BASE_URL}/updates", headers=HEADERS, timeout=10.0)
                if response.status_code == 200:
                    data = response.json()

                    # МАКС может вернуть массив напрямую или завернуть его в ключ 'updates'
                    updates_list = data if isinstance(data, list) else data.get("updates", [])

                    if updates_list:
                        for update in updates_list:
                            asyncio.create_task(process_message(update, pool))
                elif response.status_code == 401:
                    print("❌ Ошибка МАКС: Неверный токен")
                    await asyncio.sleep(10)
            except Exception as e:
                print(f"💥 Ошибка сети в цикле Long Polling: {e}")

            await asyncio.sleep(1)
