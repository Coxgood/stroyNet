# main_fastapi.py
import asyncio
import uvicorn
from fastapi_app import app
import listener_v001

async def start_fastapi():
    """Запуск веб-сервера FastAPI на порту 8000."""
    # Передаем "fastapi_app:app" строкой, чтобы Uvicorn сам правильно
    # импортировал и развернул приложение со всеми lifespan-событиями
    config = uvicorn.Config("fastapi_app:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    """Параллельный запуск шлюза и единственного Long Polling лисенера."""
    print("🚀 Инициализация экосистемы StroyNet...")
    await asyncio.gather(
        start_fastapi(),
        listener_v001.main()
    )

if __name__ == "__main__":
    asyncio.run(main())