# listener_v001.py — отладочная версия с логированием

import asyncio
import aiohttp
import asyncpg
import json
import os
from dotenv import load_dotenv

load_dotenv()

from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME, MAX_TOKEN, MAX_BASE_URL

BASE_URL = MAX_BASE_URL

print("🚀 Listener запущен (отладочная версия)")


async def ensure_employee_exists(pool, user_id, first_name, last_name):
    # ... (без изменений)
    pass


async def save_inbound_log(pool, chat_id, messenger_uid, text, **kwargs):
    # ... (без изменений)
    pass


async def process_updates(updates, pool):
    # ... (без изменений)
    pass


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
            try:
                params = {"timeout": 30}
                if marker:
                    params["marker"] = marker

                print(f"📡 Запрос к {BASE_URL}/updates (marker={marker})")
                async with session.get(f"{BASE_URL}/updates", params=params) as response:
                    print(f"📊 Статус ответа: {response.status}")

                    if response.status == 200:
                        updates = await response.json()
                        print(f"📦 Получено обновлений: {len(updates.get('updates', []))}")
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлен.")