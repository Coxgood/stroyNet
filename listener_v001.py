# listener_v001.py
import os
import asyncio
import httpx
import asyncpg

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


async def process_message(update, pool):
    """Конвейер ИИ: Уровень 1, 2, 3..."""
    # МАКС присылает сообщение в поле message или sender
    message_obj = update.get("message", {})
    chat_obj = message_obj.get("chat", {})

    # Жесткий фильтр групп
    if chat_obj.get("type") in ["group", "supergroup"]:
        return

    text = message_obj.get("text", "").strip()
    # В МАКС ID пользователя лежит в message['from']['id']
    sender_obj = message_obj.get("from", {})
    messenger_uid = str(sender_obj.get("id", ""))

    if not text or not messenger_uid:
        return

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
            print(f"❌ [Уровень 2] Доступ запрещен для UID: {messenger_uid}")
            await conn.execute(
                "UPDATE message_logs SET validation_level=2, is_valid=FALSE, intent_type='unauthorized' WHERE log_id=$1;",
                log_id)
            return

        print(f"✅ [Уровень 2] Доступ разрешен для UID: {messenger_uid}")
        await conn.execute("UPDATE message_logs SET validation_level=2, is_valid=TRUE WHERE log_id=$1;", log_id)

        # Сюда завтра мы добавим Уровень 3 и Уровень 4 (Ollama JSON Экстрактор)


async def main():
    print("🎙️ Запуск фонового лисенера StroyNet на Long Polling...")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)

    # МАКС требует отключить вебхуки перед использованием Long Polling
    async with httpx.AsyncClient(verify=False) as client:
        try:
            # Чистим старые подписки
            await client.post(f"{MAX_BASE_URL}/subscriptions", headers=HEADERS, json={"webhook_url": ""})
        except Exception as e:
            print(f"⚠️ Предупреждение при очистке вебхука: {e}")

        # Бесконечный цикл опроса МАКС
        while True:
            try:
                response = await client.get(f"{MAX_BASE_URL}/updates", headers=HEADERS, timeout=30.0)
                if response.status_code == 200:
                    updates = response.json()  # Ожидаем массив обновлений
                    if isinstance(updates, list) and updates:
                        for update in updates:
                            asyncio.create_task(process_message(update, pool))
                elif response.status_code == 401:
                    print("❌ Ошибка МАКС: Неверный токен (401)")
                    await asyncio.sleep(10)
                else:
                    print(f"⚠️ Статус МАКС: {response.status_code}")
            except Exception as e:
                print(f"💥 Ошибка сети/пула: {e}")

            await asyncio.sleep(1)  # Пауза между пуллами


if __name__ == "__main__":
    asyncio.run(main())
