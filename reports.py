# reports.py
import asyncpg
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
from config import DATABASE_URL
from ollama_client import parse_with_ollama

CHAT_ID = '-72493697010953'  # ID общего чата прорабов
LIMIT = 20  # количество сообщений для анализа

async def get_chat_log(limit: int = LIMIT) -> list:
    """Собирает последние N сообщений из группового чата."""
    query = """
        SELECT 
            m.text, 
            m.created_at,
            e.first_name || ' ' || e.last_name AS full_name,
            o.title AS organization
        FROM message_logs m
        LEFT JOIN employees e ON e.phone = 'max_' || m.messenger_uid
        LEFT JOIN employment emp ON e.employee_id = emp.employee_id
        LEFT JOIN organizations o ON emp.organization_id = o.organization_id
        WHERE m.direction = 'inbound'
          AND m.chat_id = $1
          AND (m.text ILIKE '%бетон%' OR m.text ILIKE '%раствор%')
        ORDER BY m.created_at DESC
        LIMIT $2;
    """
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, CHAT_ID, limit)
    return rows


async def parse_message_to_json(text: str) -> list:
    """Превращает одно сообщение в список JSON-заявок."""
    prompt = f"""
Ты — парсер строительных заявок. Преврати сообщение в JSON.

Сообщение: {text}

Правила:
1. Если в сообщении несколько заявок (на обед и вечер), верни список из нескольких JSON-объектов.
2. Для каждой заявки укажи: shift (утро/обед/вечер), date (ДД.ММ), sections (массив с name и volume).
3. Если заявка не распознана — верни {"error": "не распознано"}.

Формат ответа (список):
[
  {"shift": "обед", "date": "31.08", "sections": [{"name": "6 бс", "volume": 1.5}]},
  {"shift": "вечер", "date": "31.08", "sections": [{"name": "7-8 бс", "volume": 2.75}]}
]
"""
    reply = await parse_with_ollama(prompt, max_tokens=400, temperature=0.1)
    try:
        # Пытаемся извлечь JSON из ответа
        start = reply.find('[')
        end = reply.rfind(']') + 1
        if start != -1 and end > start:
            return json.loads(reply[start:end])
        return []
    except:
        return []


async def save_outbound_message(text: str):
    """Сохраняет сообщение в outbound_messages."""
    query = """
        INSERT INTO outbound_messages (platform, chat_id, messenger_uid, text, status)
        VALUES ('max', '436624187', '266417155', $1, 'pending');
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(query, text)
        print("📤 Сообщение сохранено в outbound_messages")
    finally:
        await conn.close()


async def send_report(shift: str, until_hour: int, target_day: str = "today"):
    print(f"📊 Запуск отчёта на {shift} (до {until_hour}:00)")

    rows = await get_chat_log(limit=LIMIT)
    if not rows:
        await save_outbound_message("📭 Сообщений не найдено.")
        return

    # Шаг 1: Парсим каждое сообщение в JSON
    all_orders = []
    for row in rows:
        text = row['text']
        print(f"🔍 Парсим: {text[:50]}...")
        parsed = await parse_message_to_json(text)
        if parsed:
            all_orders.extend(parsed)
            print(f"   ✅ Распознано: {parsed}")
        else:
            print(f"   ⚠️ Не распознано")

    if not all_orders:
        await save_outbound_message("📭 Заявок не найдено.")
        return

    # Шаг 2: Агрегация по дате, смене, подрядчику и секции
    summary = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

    for order in all_orders:
        date = order.get('date', 'unknown')
        shift = order.get('shift', 'unknown')
        sections = order.get('sections', [])
        for sec in sections:
            name = sec.get('name', 'unknown')
            volume = sec.get('volume', 0)
            if volume > 0:
                summary[date][shift][name] += volume

    # Шаг 3: Формируем отчёт
    lines = []
    for date in sorted(summary.keys()):
        lines.append(f"\n📅 Дата: {date}")
        for shift_name in sorted(summary[date].keys()):
            total_shift = sum(summary[date][shift_name].values())
            lines.append(f"  🕒 Смена: {shift_name.upper()} (итого: {total_shift:.2f} м³)")
            for sec, vol in summary[date][shift_name].items():
                lines.append(f"    - {sec}: {vol:.2f} м³")
            if total_shift > 4:
                lines.append(f"    ⚠️ Аномалия: {total_shift:.2f} м³ > 4")

    report_text = "\n".join(lines)
    await save_outbound_message(f"📋 **СВОДКА ЗАЯВОК**\n\n{report_text}")
    print("✅ Отчёт отправлен!")