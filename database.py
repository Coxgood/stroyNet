import asyncpg
from typing import Optional
from config import DATABASE_URL

class DBManager:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(dsn=DATABASE_URL, min_size=2, max_size=10)
            print("🟢 Пул подключений к локальной PostgreSQL запущен.")

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            print("🔌 Пул подключений к PostgreSQL закрыт.")

    async def log_incoming_message(self, platform: str, chat_id: str, chat_type: str,
                                   messenger_uid: str, text: str, message_uid: Optional[str] = None,
                                   intent_type: str = 'unknown', confidence_score: int = 0,
                                   priority: int = 1, validation_level: int = 1,
                                   validation_score: int = 50, source_type: str = 'text',
                                   access_level: int = 1) -> None:
        """Запись любого входящего сообщения в message_logs со всеми полями."""
        if not self.pool:
            raise Exception("БД не подключена")
        query = """
            INSERT INTO message_logs (
                platform, chat_id, chat_type, messenger_uid, direction, message_uid, text,
                intent_type, confidence_score, priority,
                validation_level, validation_score, source_type, access_level
            )
            VALUES ($1, $2, $3, $4, 'inbound', $5, $6, $7, $8, $9, $10, $11, $12, $13);
        """
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                platform, str(chat_id), chat_type, str(messenger_uid),
                message_uid, text,
                intent_type, confidence_score, priority,
                validation_level, validation_score, source_type, access_level
            )

    async def check_user_exists_by_uid(self, messenger_uid: str) -> bool:
        """🛡️ УРОВЕНЬ 2: Проверка допуска прораба (Демо-заглушка).

        В таблице employee_phones пока нет колонки messenger_uid,
        поэтому для тестов временно одобряем всех авторизованных в МАКС прорабов.
        """
        # Как только добавите в employee_phones привязку к UID МАКС/Телеграм,
        # здесь будет полноценный SQL-запрос.
        return True

    # =====================================================================
    # РЕГИСТРАЦИЯ НОВОГО ПОЛЬЗОВАТЕЛЯ
    # =====================================================================
    async def register_user(self, platform: str, user_id: str, first_name: str) -> int:
        """
        Регистрирует нового пользователя:
        1. Проверяет, есть ли уже такой сотрудник в employees (по phone)
        2. Если нет — создаёт нового.
        3. Создаёт запись в employee_accounts.
        """
        if not self.pool:
            raise Exception("БД не подключена")

        phone = f"{platform}_{user_id}"
        async with self.pool.acquire() as conn:
            # проверяем, есть ли уже сотрудник
            existing = await conn.fetchval(
                "SELECT employee_id FROM employees WHERE phone = $1",
                phone
            )
            if existing:
                # если есть — создаём только аккаунт
                await conn.execute(
                    "INSERT INTO employee_accounts (employee_id, platform, messenger_uid) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    existing, platform, str(user_id)
                )
                return existing

            # создаём нового сотрудника
            new_id = await conn.fetchval(
                """
                INSERT INTO employees (first_name, last_name, phone, telegram_uid)
                VALUES ($1, '', $2, 'не указан')
                RETURNING employee_id
                """,
                first_name, phone
            )
            # создаём аккаунт
            await conn.execute(
                "INSERT INTO employee_accounts (employee_id, platform, messenger_uid) VALUES ($1, $2, $3)",
                new_id, platform, str(user_id)
            )
            print(f"🆕 Зарегистрирован новый пользователь: {first_name} (ID: {user_id})")
            return new_id

    # =====================================================================
    # СОХРАНЕНИЕ РАСПАРСЕННОЙ ЗАЯВКИ В orders
    # =====================================================================
    async def save_parsed_order(self, messenger_uid: str, raw_text: str, parsed_data: dict) -> None:
        """Сохраняет распарсенную заявку в таблицу orders."""
        if not self.pool:
            raise Exception("БД не подключена")

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orders (employee_id, raw_text, ai_structured, status)
                VALUES (
                    (SELECT employee_id FROM employees WHERE phone = 'max_' || $1),
                    $2,
                    $3,
                    'parsed'
                )
                """,
                messenger_uid, raw_text, parsed_data
            )

    # =====================================================================
    # СОХРАНЕНИЕ НЕРАСПОЗНАННОЙ ЗАЯВКИ (ОШИБКА)
    # =====================================================================
    async def save_unprocessed_order(self, messenger_uid: str, raw_text: str, error: str) -> None:
        """Сохраняет заявку, которую не удалось распарсить."""
        if not self.pool:
            raise Exception("БД не подключена")

        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orders (employee_id, raw_text, status, ai_structured)
                VALUES (
                    (SELECT employee_id FROM employees WHERE phone = 'max_' || $1),
                    $2,
                    'error',
                    jsonb_build_object('error', $3)
                )
                """,
                messenger_uid, raw_text, error
            )
    async def update_validation_status(self, log_id: int, level: int, is_valid: bool, score: int, intent: str):
        """📊 УРОВЕНЬ 3: Обновляет стадию валидации и результаты лингвистического анализа."""
        if not log_id:
            return

        query = """
            UPDATE message_logs
            SET validation_level = $1,
                is_valid = $2,
                validation_score = $3,
                intent_type = $4
            WHERE log_id = $5;
        """
        try:
            # Вызываем execute через ваш пул подключений
            await self.pool.execute(query, level, is_valid, score, intent, log_id)
            print(f"🎯 [БД] Статус лога #{log_id} успешно обновлен на уровень {level}")
        except Exception as e:
            print(f"❌ Ошибка метода update_validation_status для лога #{log_id}: {e}")



db = DBManager()
