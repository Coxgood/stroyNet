import asyncio
import uvicorn
import listener_v001
from fastapi_app import app, listen_for_messages


async def start_fastapi():
    """Запуск веб-сервера FastAPI на порту 8000."""
    config = uvicorn.Config("fastapi_app:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    print("🚀 Инициализация экосистемы StroyNet...")

    # Запускаем параллельно веб-сервер, опрос MAX и слушателя БД
    await asyncio.gather(
        start_fastapi(),
        listener_v001.main(),
        listen_for_messages()
    )


if __name__ == "__main__":
    asyncio.run(main())
