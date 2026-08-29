import asyncpg
from datetime import datetime
from config import DATABASE_URL
from ollama_client import parse_with_ollama


async def get_latest_chat_log(limit: int = 30) -> tuple[str | None, datetime | None]:
    """
    Выгружает ровно N последних входящих сообщений без жесткой привязки к датам.
    Фильтр по ключевым словам убран, чтобы не потерять сообщения 'Отмена' и 'Корректировка'.
    """
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
        ORDER BY m.created_at DESC
        LIMIT $1;
    """
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)

    if not rows:
        return None, None

    # Разворачиваем в хронологический порядок (было DESC для LIMIT, делаем ASC для ИИ)
    rows = list(reversed(rows))

    # Находим самый свежий таймстемп в этой выборке (точка актуальности чата)
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
    async with asyncpg.create_pool(DATABASE_URL) as pool:
        async with pool.acquire() as conn:
            await conn.execute(query, text)
            print("📤 Сообщение сохранено в outbound_messages")


async def send_report(shift: str, until_hour: int):
    """Формирует и отправляет оперативный отчёт на основе скользящего окна чата."""
    print(f"📊 Запуск отчёта под слот: {shift}")

    # Получаем срез последних сообщений и реальное время последней активности в чате
    chat_log, last_active_time = await get_latest_chat_log(limit=30)

    if not chat_log or not last_active_time:
        await save_outbound_message(f"📭 В чате нет сообщений для формирования отчета на {shift}.")
        return

    # Логика выходных: проверяем день недели относительно ПОСЛЕДНЕГО сообщения, а не сервера.
    # Если база замерла в субботу, мы обрабатываем субботний контекст, а не пустой понедельник.
    weekday_num = last_active_time.weekday()  # 5 - Суббота, 6 - Воскресенье

    # Если вы не работаете по воскресеньям, блокируем генерацию
    if weekday_num == 6:
        await save_outbound_message(
            f"📅 По данным чата сейчас выходной день ({last_active_time.strftime('%A, %Y-%m-%d')}). "
            f"Отчёт на {shift} не формируется."
        )
        return

    prompt = f"""
Ты — «StroyNet Диспетчер», жесткий и опытный ИИ-инженер строительной логистики. 
Твоя задача — собрать финальные, актуальные заявки на БЕТОН и РАСТВОР под целевой рабочий слот: {shift}.

Вот выписка последних 30 сообщений из чата (точка актуальности чата: {last_active_time.strftime('%Y-%m-%d %H:%M')}):
{chat_log}

ИНСТРУКЦИЯ ПО АНАЛИЗУ ЧАТА:
1. Выдели только целевые заявки на БЕТОН и РАСТВОР, которые относятся к слоту "{shift}" (например, если слот ОБЕД, ищи маркеры "на обед", "к обеду", "12:00").
2. Строго сопоставляй таймстемпы сообщений. Если прораб написал "на утро" вечером, это заявка на утро следующего дня.
3. Разреши противоречия (ЦЕПОЧКА ВРЕМЕНИ):
   - Если за заявкой от конкретного прораба следует сообщение "Отмена" или "Корректировка" от него же — примени изменения.
   - Схлопни промежуточный хаос. В отчёт должно попасть только финальное решение человека. Отрезанные/отмененные заявки полностью удаляй.
4. Сгруппируй результат по Подрядчикам (Организациям) и Конструкциям/Секциям, просуммируй объемы в м³.
5. Если целевых заявок на "{shift}" нет или они все были аннулированы — верни ровно одну строку: "Заявок на {shift} за период не найдено."

Выдай сухой, структурированный технический отчет без вежливости, смайликов и вводных слов.

Отчёт:
"""

    reply = await parse_with_ollama(prompt)
    await save_outbound_message(f"📋 **СВОДКА НА {shift.upper()}**\n\n{reply}")
