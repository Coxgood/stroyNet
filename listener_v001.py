# listener_v001.py
import os
import asyncio
import httpx
import asyncpg
import json

# Конфигурация из .env
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "GlDxzFUy6V")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stroy_net")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MAX_BASE_URL = os.getenv("MAX_BASE_URL", "https://platform-api2.max.ru")
MAX_TOKEN = os.getenv("MAX_TOKEN",
                      "f9LHodD0cOKvY0QG7ysxONTv-IGk5yxXNd_-7VCRjAqGI5SPN3KSnA9vMF5SKtVeN5ZuDafkyCqoSWUV4O6t")

HEADERS = {"Authorization": MAX_TOKEN}  # Важно: без Bearer!

# listener_v001.py (Обновленный фрагмент кода)
import json  # Не забудьте импортировать в самом верху файла!


async def process_message(update, pool):
    """Конвейер ИИ: Безопасный парсинг сырого обновления от МАКС."""
    try:
        # Если МАКС прислал строку, принудительно превращаем её в JSON-словарь
        if isinstance(update, str):
            try:
                update = json.loads(update)
            except Exception as e:
                print(f"⚠️ Не удалось распарсить строку апдейта в JSON: {e}. Данные: {update}")
                return

        # Теперь мы железно уверены, что update — это словарь
        if not isinstance(update, dict):
            print(f"⚠️ Нетипичный формат апдейта МАКС (не dict): {type(update)}")
            return

        message_obj = update.get("message", {})
        if not message_obj:
            # На случай, если МАКС прислал плоскую структуру
            message_obj = update

        chat_obj = message_obj.get("chat", {})

        # Жесткий фильтр групп
        if isinstance(chat_obj, dict) and chat_obj.get("type") in ["group", "supergroup"]:
            return

        text = message_obj.get("text", "").strip()

        # Извлекаем UID (проверяем структуру вложенности МАКС)
        sender_obj = message_obj.get("from", {})
        if isinstance(sender_obj, dict):
            messenger_uid = str(sender_obj.get("id", ""))
        else:
            messenger_uid = str(message_obj.get("user_id") or update.get("messenger_uid", ""))

        if not text or not messenger_uid:
            return

        # --- Далее идет ваша рабочая SQL логика пула (Уровни 1 и 2) ---
        async with pool.acquire() as conn:
            # Уровень 1: Логирование в БД
            query = """
                INSERT INTO message_logs (messenger_uid, text, validation_level, is_valid, intent_type)
                VALUES ($1, $2, 1, FALSE, 'unprocessed')
                RETURNING log_id;
            """
            log_id = await conn.fetchval(query, messenger_uid, text)
            print(f"📥 [Уровень 1] Сообщение #{log_id} сохранено в БД.")

            # Уровень 2: Проверка допуска прораба
            has_access = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM employee_phones WHERE messenger_uid = $1);",
                messenger_uid
            )

            if not has_access:
                print(f"❌ [Уровень 2] Доступ запрещен для UID: {messenger_uid}. Запишите его в базу для тестов!")
                await conn.execute("""
                    UPDATE message_logs 
                    SET validation_level = 2, is_valid = FALSE, intent_type = 'unauthorized' 
                    WHERE log_id = $1;
                """, log_id)
                return

            print(f"✅ [Уровень 2] Доступ разрешен для UID: {messenger_uid}")
            await conn.execute("""
                UPDATE message_logs 
                SET validation_level = 2, is_valid = TRUE 
                WHERE log_id = $1;
            """, log_id)

    except Exception as e:
        print(f"💥 Критическая ошибка парсинга внутри process_message: {e}")


async def main():
    print("🎙️ Запуск ЕДИНСТВЕННОГО фонового лисенера StroyNet на Long Polling...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    async with httpx.AsyncClient(verify=False) as client:
        while True:
            try:
                # Прямо дергаем МАКС без всяких триггеров БД!
                response = await client.get(f"{MAX_BASE_URL}/updates", headers=HEADERS, timeout=30.0)
                if response.status_code == 200:
                    updates = response.json()
                    if updates:
                        for update in updates:
                            # Прямо отправляем на конвейер
                            asyncio.create_task(process_message(update, pool))
            except Exception as e:
                print(f"💥 Ошибка: {e}")
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
