# filename: main_fastapi.py
# description: Главная точка запуска FastAPI на сервере

import uvicorn
from fastapi_app import app

if __name__ == "__main__":
    # Запускаем uvicorn на порту 8000
    uvicorn.run("fastapi_app:app", host="0.0.0.0", port=8000, reload=False)
