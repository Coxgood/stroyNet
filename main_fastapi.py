import asyncio
import uvicorn
import asyncpg
import aiohttp
import listener_v001
# Добавляем прямой импорт функции нашего слушателя триггеров из fastapi_app
from fastapi_app import listen_for_messages
import os
from dotenv import load_dotenv

# Загружаем переменные из вашего скрытого файла .env на сервере
load_dotenv()

# Извлекаем параметры подключения к СУБД
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "stroy_net")

# Токен платформы MAX и базовый URL шлюза
MAX_TOKEN = os.getenv("MAX_TOKEN")
BASE_URL = os.getenv("BASE_URL", "https://max.ru")

async def start_fastapi():
    """Запуск веб-сервера FastAPI на порту 8000."""
    config = uvicorn.Config("fastapi_app:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    print("🔌 Подключение к БД...")
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
    print(f"🔗 Подключаюсь к MAX: {BASE_URL}")

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        marker = None
        print("🚀 Начинаю опрос MAX...")

        while True:
            # === 🎯 НАШ НОВЫЙ ИСПРАВЛЕННЫЙ ШАГ ===
            # Перед тем как уйти в длинный опрос входящих обновлений MAX,
            # лиснер мгновенно проверяет нашу новую таблицу и выталкивает ответы ИИ прорабам
            await send_ai_responses_from_queue(session, pool)
            # ======================================

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
                        print("⚠️ Конфликт сессий (409)")
                        await asyncio.sleep(5)
                    else:
                        print(f"⚠️ Статус: {response.status}")
                        await asyncio.sleep(2)
            except Exception as e:
                print(f"💥 Ошибка в цикле: {e}")
                await asyncio.sleep(3)

            await asyncio.sleep(0.5)


if __name__ == "__main__":
    asyncio.run(main())