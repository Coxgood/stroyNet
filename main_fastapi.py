import asyncio
import uvicorn
import listener_v001
# Добавляем прямой импорт функции нашего слушателя триггеров из fastapi_app
from fastapi_app import listen_for_messages

async def start_fastapi():
    """Запуск веб-сервера FastAPI на порту 8000."""
    config = uvicorn.Config("fastapi_app:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

async def main():
    """Параллельный запуск всех компонентов монолита в одном Event Loop."""
    print("🚀 Инициализация экосистемы StroyNet...")
    await asyncio.gather(
        start_fastapi(),         # 1. Веб-сервер
        listener_v001.main(),    # 2. Long Polling транспорт до MAX
        listen_for_messages()    # 3. НАШ СЛУШАТЕЛЬ ТРИГГЕРОВ БД
    )

if __name__ == "__main__":
    asyncio.run(main())