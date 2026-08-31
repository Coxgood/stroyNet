# reports.py
import asyncpg
from datetime import datetime, timedelta
from config import DATABASE_URL
from ollama_client import parse_with_ollama

CHAT_ID = '-72493697010953'  # ID общего чата прорабов


async def get_chat_log(limit: int = 30) -> tuple:
    """Собирает последние N сообщений из группового чата (без фильтра по дате)."""
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

    if not rows:
        return None, None

    chat_log = "\n".join([
        f"[{row['created_at']}] {row['full_name']} ({row['organization']}): {row['text']}"
        for row in rows
    ])

    return chat_log, rows[0]['created_at'] if rows else None


async def save_outbound_message(text: str):
    """Сохраняет сообщение в outbound_messages для отправки."""
    query = """
        INSERT INTO outbound_messages (platform, chat_id, messenger_uid, text, status)
        VALUES ('max', '436624187', '266417155', $1, 'pending');
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute(query, text)
        print("📤 Сообщение успешно сохранено в outbound_messages")
    finally:
        await conn.close()


async def send_report(shift: str, until_hour: int, target_day: str = "today"):
    """Формирует отчёт на основе времени запуска."""
    print(f"📊 Запуск отчёта на {shift} (до {until_hour}:00, день: {target_day})")

    today = datetime.now().date()
    if target_day == "tomorrow":
        target_date = today + timedelta(days=1)
    else:
        target_date = today

    keyword = "завтра" if target_day == "tomorrow" else "сегодня"

    chat_log, last_time = await get_chat_log(limit=12)

    if not chat_log:
        await save_outbound_message(f"📭 Заявок на {shift} ({target_day}) не найдено.")
        return

    # Проверяем объёмы (баг-детектор)
    volumes = []
    import re
    for line in chat_log.split('\n'):
        numbers = re.findall(r'(\d+[,.]?\d*)\s*м[3³]', line)
        for v in numbers:
            try:
                vol = float(v.replace(',', '.'))
                volumes.append(vol)
            except:
                pass

    if volumes:
        avg_vol = sum(volumes) / len(volumes)
        max_vol = max(volumes)
        print(f"🔍 Объёмы: {volumes}")
        print(f"🔍 Средний: {avg_vol:.2f} м³, Максимальный: {max_vol:.2f} м³")
        if max_vol > 6:
            print("⚠️ Обнаружен аномальный объём (>6 м³)")
        elif max_vol > 3:
            print("⚠️ Объём выше среднего (>3 м³)")

    prompt = fprompt = f"""
Ты — автоматическая система генерации отчетов бетонного завода. Твоя задача — проанализировать предоставленный лог сообщений и составить структурированный отчет ПО ВСЕМ ДАТАМ И ВСЕМ СМЕНАМ, которые встречаются в тексте.

=== СЛОВАРЬ КЛАССИФИКАЦИИ СМЕН ===
Распределяй заявки по сменам на основе времени или ключевых слов:
- УТРО: маркеры "утро", "8:00", "8.00"
- ОБЕД: маркеры "обед", "13:00", "13.00"
- ВЕЧЕР: маркеры "вечер", "15:00", "15.00", "15:30"
Если дата в сообщении явно указана как "завтра" или "на 27.08", относи запись к указанной календарной дате.

=== ЖЁСТКИЕ ПРАВИЛА АНАЛИЗА ===
1. ХРОНОЛОГИЯ: Лог идет снизу вверх (старые записи внизу, новые вверху). Анализируй снизу вверх, чтобы корректно учитывать корректировки и отмены.
2. НАЗВАНИЯ СЕКЦИЙ: В финальном отчете используй ТОЛЬКО следующие стандартизированные названия:
   - "6 бс"
   - "7-8 бс"
   - "4,5 секция"
   - "1бс"
   - "2бс"
3. ОБЪЕМЫ: Извлекай числа только перед "м³" или "м3". Если для "4,5 секция" указаны метры со звездочкой (например, "3м*"), этот объем раствора равен 0 м³ (это строительная отметка, а не кубатура).
4. ОТМЕНЫ: Если видишь слова "отмена" или "отказ" для конкретной секции/даты/смены, её объем равен 0 м³. Например, "7-8 бс раствор отмена" обнуляет заявку этой секции на эту смену.
5. КОРРЕКТИРОВКИ: Если на одну и ту же дату, смену и секцию есть несколько записей, бери только самую последнюю (верхнюю в логе). Предыдущие значения полностью игнорируй.

=== ФОРМАТ ОТЧЁТА ===
Выводи данные последовательно по датам (в хронологическом порядке: от старых к новым). Для каждой даты разделяй отчет по сменам (УТРО, ОБЕД, ВЕЧЕР). Используй строго следующий шаблон:

Дата: [ДД.ММ]
Смена: [УТРО / ОБЕД / ВЕЧЕР]
  Подрядчик: [Название подрядчика, например: ИП Слепченко. Если не указан — "Общие заявки"]
    Секция [Название]: [объём] м³
    Итого по подрядчику: [сумма] м³
  
  Общий итог за смену: [сумма всех м³ за эту смену] м³
  Аномалии (>4 м³): [- секция, объём. Если нет — "Нет"]
  ---

=== ЛОГ ДЛЯ АНАЛИЗА ===
"""

    print("🔍 Отправляем в Ollama...")
    reply = await parse_with_ollama(prompt)
    print(f"🔍 Ответ получен, длина: {len(reply)} символов")

    await save_outbound_message(f"📋 **СВОДКА НА {shift.upper()} ({target_day})**\n\n{reply}")
    print("✅ Отчёт отправлен!")