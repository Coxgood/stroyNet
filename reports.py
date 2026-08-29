import asyncpg
from datetime import datetime
from config import DATABASE_URL
from ollama_client import parse_with_ollama


async def get_latest_chat_log(limit: int = 30) -> tuple[str | None, datetime | None]:
    """
    Выгружает последние N профильных сообщений СТРОГО из групповых чатов и каналов,
    полностью игнорируя личные диалоги ('dialog').
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
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
              AND m.chat_type IN ('group', 'channel') -- 🚨 ФИЛЬТР: Сбор только из групп
              AND (
                   m.text ILIKE '%бетон%' 
                OR m.text ILIKE '%раствор%' 
                OR m.text ILIKE '%пб%'
                OR m.text ILIKE '%плит%'
                OR m.text ILIKE '%отмен%' 
                OR m.text ILIKE '%коррект%'
              )
            ORDER BY m.created_at DESC
            LIMIT $1;
        """
        rows = await conn.fetch(query, limit)
    finally:
        await conn.close()

    if not rows:
        return None, None

    # Хронологический порядок для ИИ
    rows = list(reversed(rows))
    last_msg_timestamp = max(row['created_at'] for row in rows)

    lines = []
    for row in rows:
        name = row['full_name'] or 'Неизвестный'
        org = row['organization'] or 'Не указана'
        lines.append(f"[{row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}] {name} ({org}): {row['text']}")

    return "\n".join(lines), last_msg_timestamp


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


async def send_report(shift: str, until_hour: int):
    """Формирует оперативный технический отчет по данным групповых чатов."""
    print(f"📊 Запуск генерации отчёта по ГРУППАМ под слот: {shift}")

    chat_log, last_active_time = await get_latest_chat_log(limit=30)

    if not chat_log or not last_active_time:
        await save_outbound_message(f"📭 В групповых чатах не обнаружено активных заявок на бетон/раствор.")
        return

    if last_active_time.weekday() == 6:
        await save_outbound_message(
            f"📅 Согласно логам групп сейчас воскресенье ({last_active_time.strftime('%Y-%m-%d')}). "
            f"Отчёт на {shift} не формируется."
        )
        return

    prompt = f"""
Ты — «StroyNet Диспетчер», автоматизированный ИИ-модуль учета строительной логистики. 
Твоя цель: составить сводный отчет по материалам на основе выписки из ГРУППОВЫХ чатов.

ЦЕЛЕВОЙ СЛОТ ДОСТАВКИ: {shift.upper()}
ТОЧКА АКТУАЛЬНОСТИ ЛОГА (Последнее групповое сообщение): {last_active_time.strftime('%Y-%m-%d %H:%M')}

ВЫПИСКА ИЗ ОБЩИХ ЧАТОВ ПРОРАБОВ:
{chat_log}

ОБЯЗАТЕЛЬНЫЙ АЛГОРИТМ ОБРАБОТКИ:
1. Выпиши ВСЕ упоминания бетона, раствора или ЖБИ-плит.
2. Проверь время доставки, которое запрашивает прораб:
   - Если указано "на утро", "к 8 часам", "08:00" -> это слот УТРО.
   - Если указано "на обед", "к 12:00", "12:00" -> это слот ОБЕД.
   - Если прораб пишет поздно вечером "на утро" -> это заявка на утро следующего дня.
3. ОБРАБОТКА ОТМЕН И КОРРЕКТИРОВОК:
   - Если один и тот же прораб сначала заказал материал, а затем написал "Отмена" или "Корректировка" -> примени изменения и удали аннулированную позицию.
4. Выведи результат строго в формате:
   Подрядчик (Организация): [Название]
   Материал/Марка: [Что везем]
   Объем: [Количество] шт/кубов
   Конструкция/Захватка: [Куда укладываем, если указано]

Внимание: Если в предоставленном логе действительно нет ни одной живой (не отмененной) заявки на слот {shift.upper()}, только тогда верни фразу: "Заявок на {shift} за период не найдено."

Отчёт:
"""

    reply = await parse_with_ollama(prompt)
    await save_outbound_message(f"📋 **СВОДКА ИЗ ГРУПП НА {shift.upper()}**\n\n{reply}")
