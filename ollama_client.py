# filename: ollama_client.py
# description: асинхронный клиент для Ollama, возвращающий живой текст

import httpx
import json
import logging
from prompts import CHAT_SYSTEM_PROMPT

logger = logging.getLogger("uvicorn.error")
OLLAMA_API_URL = "http://localhost:11434/api/generate"

build_system_prompt = "Ты — профессиональный ИИ-диспетчер строительной компании «StroyNet». Твоя задача — принимать заявки от прорабов на строительные материалы и технику. Пожалуйста, отвечай развернуто, вежливо и понятно обычным человеческим языком. Обязательно подтверди, что ты принял запрос в обработку."



async def parse_with_ollama(
        text: str,
        messenger_uid: str = None,  # Добавляем UID для поиска контекста в БД
        mode: str = "chat",
        model_name: str = "qwen2.5:7b"
) -> str:
    """Отправляет запрос в Ollama и возвращает развернутый текстовый ответ."""

    # Проверяем: если из FastAPI уже пришел готовый собранный промпт со строкой "Ответ диспетчера:",
    # то используем его как есть, чтобы не ломать контекст Константина.
    if "Ответ диспетчера:" in text:
        final_prompt = text
    else:
        # Для всех остальных стандартных вызовов собираем классическую схему
        system_prompt = CHAT_SYSTEM_PROMPT
        final_prompt = f"{system_prompt}\n\nСообщение от прораба: {text}\nОтвет диспетчера:"

    payload = {
        "model": model_name,
        "prompt": final_prompt,  # 🚀 Передаем правильный итоговый текст
        "stream": False,
        "options": {
            "temperature": 0.4,  # Оптимально для живой, но точной речи
            "num_ctx": 4096,     # Контекстное окно (память модели)
            "num_predict": 256,  # Разрешаем модели писать нормальные предложения
            "top_p": 0.9         # Делает речь естественной
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

