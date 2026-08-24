# filename: ollama_client.py
# description: асинхронный клиент для Ollama, возвращающий живой текст

import httpx
import json
import logging
from prompts import CHAT_SYSTEM_PROMPT

logger = logging.getLogger("uvicorn.error")
OLLAMA_API_URL = "http://localhost:11434/api/generate"


async def parse_with_ollama(text: str, mode: str = "chat", model_name: str = "qwen2.5:1.5b") -> str:
    """Отправляет запрос в Ollama и ВСЕГДА возвращает живой текстовый ответ (строку)."""

    payload = {
        "model": model_name,
        "prompt": f"{CHAT_SYSTEM_PROMPT}\n\nСообщение от прораба: {text}\nОтвет диспетчера:",
        "stream": False,
        "options": {
            "temperature": 0.4,  # Оптимально для живой, но точной речи
            "num_ctx": 4096
        }
    }

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(OLLAMA_API_URL, json=payload)

            if response.status_code != 200:
                logger.error(f"❌ Ошибка Ollama API: {response.status_code}")
                return "⚠️ Диспетчер временно занят, повторите запрос чуть позже."

            reply_text = response.json().get("response", "").strip()

            # Очистка от скрытых мыслей DeepSeek, если решите включить его
            if "<think>" in reply_text:
                reply_text = reply_text.split("</think>")[-1].strip()

            return reply_text

    except Exception as e:
        logger.error(f"❌ Ошибка вызова Ollama: {e}", exc_info=True)
        return "⚠️ Не удалось связаться с диспетчером."
