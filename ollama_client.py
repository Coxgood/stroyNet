# filename: ollama_client.py
# description: клиент для вызова Ollama

import httpx
import json
from config import OLLAMA_URL, OLLAMA_MODEL
from prompts import SYSTEM_PROMPT

async def parse_with_ollama(text: str) -> str:
    """Отправляет текст в Ollama и возвращает ответ как строку."""
    prompt = f"{SYSTEM_PROMPT}\n\nСообщение: {text}\n\nОтвет:"
    async with httpx.AsyncClient() as client:
        response = await client.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "num_predict": 500,
                    "temperature": 0.7
                }
            },
            timeout=60
        )
        data = response.json()
        return data.get("response", "⚠️ Ошибка: пустой ответ от модели")