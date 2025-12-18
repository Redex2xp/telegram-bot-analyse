import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from dotenv import load_dotenv

from db import get_db_connection, execute_query
from llm import get_sql_from_llm

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()


@dp.message(CommandStart())
async def send_welcome(message: types.Message):
    """
    Функция-обработчик для команды /start.
    Отправляет приветственное сообщение.
    """
    await message.reply(
        "Привет! Я бот для аналитики по видео. "
        "Отправь мне вопрос на естественном языке, и я попробую на него ответить.\n\n"
        "Например: 'Сколько всего видео в системе?'"
    )


@dp.message()
async def handle_query(message: types.Message):
    """
    Основной обработчик для текстовых запросов пользователя.

    1. Получает текстовый запрос.
    2. Отправляет запрос к LLM для генерации SQL.
    3. Выполняет SQL-запрос к базе данных.
    4. Отправляет результат пользователю.
    """
    user_query = message.text
    logger.info(f"Получен новый запрос от пользователя: {user_query}")

    await message.answer("Думаю над вашим вопросом...")

    try:
        sql_query = await get_sql_from_llm(user_query)
        if not sql_query:
            await message.answer("Не удалось сгенерировать SQL-запрос. Попробуйте переформулировать вопрос.")
            return

        logger.info(f"Сгенерирован SQL-запрос: {sql_query}")

        db_conn = await get_db_connection()
        if db_conn is None:
            await message.answer("Не удалось подключиться к базе данных. Попробуйте позже.")
            return

        result = await execute_query(db_conn, sql_query)
        await db_conn.close()

        logger.info(f"Результат выполнения запроса: {result}")

        if result is not None:
            await message.answer(f"Ответ: {result}")
        else:
            await message.answer("Не удалось получить результат. Возможно, запрос некорректен или данные отсутствуют.")

    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}", exc_info=True)
        await message.answer("Произошла внутренняя ошибка. Пожалуйста, попробуйте позже.")


async def main():
    """
    Главная функция для запуска бота.
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.error("Токен Telegram-бота не найден. Убедитесь, что он задан в .env файле.")
        return

    logger.info("Бот запускается...")
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
