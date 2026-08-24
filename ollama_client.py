# filename: ollama_client.py
# description: асинхронный клиент для Ollama с поддержкой режимов текста и JSON

import httpx
import json
import logging

logger = logging.getLogger("uvicorn.error")
OLLAMA_API_URL = "http://localhost:11434/api/generate"


async def parse_with_ollama(text: str, mode: str = "chat", model_name: str = "qwen2.5:1.5b") -> dict | str:
    """Отправляет запрос в Ollama.

    mode="chat" — возвращает обычную строку текста.
    mode="parser" — возвращает готовый структурированный словарь Python (JSON).
    """
    # Подключаем наши новые раздельные промпты
    from prompts import CHAT_SYSTEM_PROMPT, PARSER_SYSTEM_PROMPT
    system_prompt = PARSER_SYSTEM_PROMPT if mode == "parser" else CHAT_SYSTEM_PROMPT

    payload = {
        "model": model_name,
        "prompt": f"{system_prompt}\n\nТекст пользователя: {text}\nОтвет:",
        "stream": False,
        "options": {
            "temperature": 0.1 if mode == "parser" else 0.5,  # Минимальный хаос для JSON
            "num_ctx": 4096
        }
    }

    # Если нужен жесткий JSON, заставляем саму Ollama форматировать ответ
    if mode == "parser":
        payload["format"] = "json"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(OLLAMA_API_URL, json=payload)

            if response.status_code != 200:
                return {"error": f"Ollama error {response.status_code}"} if mode == "parser" else "⚠️ Ошибка ИИ."

            reply_text = response.json().get("response", "").strip()

            # Возвращаем данные в зависимости от требуемого режима
            if mode == "parser":
                return json.loads(reply_text)  # Превращаем строку в готовый словарь Python dict
            return reply_text  # Возвращаем обычный текст для чата

    except Exception as e:
        logger.error(f"❌ Сбой Ollama в режиме {mode}: {e}", exc_info=True)
        return {"error": str(e)} if mode == "parser" else "⚠️ ИИ временно недоступен."
