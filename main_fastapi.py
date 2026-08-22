# filename: main_fastapi.py
# description: единая точка запуска FastAPI + listener (Long Polling)
# depends: fastapi_app.py, listener_v001.py, config.py, database.py
# runs_as: основной процесс (uvicorn)

import asyncio
import sys
import os
import signal
from pathlib import Path

# добавляем текущую папку в путь, чтобы импорты работали
sys.path.append(str(Path(__file__).parent))

# =====================================================================
# ИМПОРТЫ НАШИХ МОДУЛЕЙ
# =====================================================================
from fastapi_app import app
from listener_v001 import run_listener_forever
from config import MAX_TOKEN, MAX_BASE_URL as BASE_URL

# =====================================================================
# ФУНКЦИЯ ЗАПУСКА FASTAPI (через uvicorn)
# =====================================================================
async def run_fastapi():
    """Запускает FastAPI через uvicorn в том же процессе."""
    import uvicorn
    print("🌐 Запуск FastAPI на порту 8000...")
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


# =====================================================================
# ОБРАБОТЧИК ЗАВЕРШЕНИЯ (Ctrl+C)
# =====================================================================
def signal_handler(sig, frame):
    print("\n🛑 Получен сигнал остановки. Завершаем работу...")
    sys.exit(0)


# =====================================================================
# ГЛАВНАЯ ФУНКЦИЯ (запускает FastAPI и Listener параллельно)
# =====================================================================
async def main():
    # проверяем токен MAX перед запуском
    if not MAX_TOKEN:
        print("❌ Ошибка: MAX_TOKEN не задан в config.py или .env")
        sys.exit(1)

    print("🚀 Запуск StroyNet Dispatcher...")
    print(f"📡 Listener будет опрашивать: {BASE_URL}")
    print(f"🔑 Токен MAX: {MAX_TOKEN[:10]}...")

    # регистрируем обработчик Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # запускаем FastAPI и Listener параллельно
    # listener будет работать в фоне, FastAPI — принимать запросы
    await asyncio.gather(
        run_fastapi(),
        run_listener_forever(),
        return_exceptions=True
    )


# =====================================================================
# ЗАПУСК
# =====================================================================
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем")
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        sys.exit(1)