import asyncio
import logging
import os
import re

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


async def swap_video_id_to_creator_id(query: str) -> str:
    """
    Функция ищет UUID в запросе, проверяет, является ли он ID видео,
    и если да, заменяет его на ID креатора.
    """
    # Ищем UUID в запросе
    match = re.search(r'([a-f\d]{8}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{12})', query)
    if not match:
        return query

    entity_id_str = match.group(1)
    logger.info(f"Найден UUID: {entity_id_str}")

    db_conn = await get_db_connection()
    if db_conn is None:
        logger.error("Не удалось подключиться к базе данных для проверки ID.")
        return query  # Возвращаем исходный запрос, если нет подключения

    try:
        # Ищем видео
        creator_id_from_video = await execute_query(db_conn, f"SELECT creator_id FROM videos WHERE id = '{entity_id_str}'")
        if creator_id_from_video:
            logger.info(f"UUID является ID видео. Найден ID креатора: {creator_id_from_video}")
            # Заменяем ID видео на ID креатора в запросе
            return query.replace(entity_id_str, str(creator_id_from_video))
        
        # Если не является ID видео, то предполагаем, что это ID креатора или не имеет отношения к видео/креаторам
        logger.info("Найденный UUID не является ID видео. Предполагаем, что это ID креатора или не относится к видео.")

    except Exception as e:
        logger.error(f"Ошибка при проверке ID: {e}", exc_info=True)
    finally:
        if db_conn:
            await db_conn.close()

    return query


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
    2. Проверяет UUID и при необходимости заменяет video_id на creator_id.
    3. Отправляет запрос к LLM для генерации SQL.
    4. Выполняет SQL-запрос к базе данных.
    5. Отправляет результат пользователю.
    """
    user_query = message.text
    logger.info(f"Получен новый запрос от пользователя: {user_query}")



    try:
        # Шаг 2: Проверка и замена ID
        modified_query = await swap_video_id_to_creator_id(user_query)
        if modified_query != user_query:
            logger.info(f"Запрос был изменен: {modified_query}")

        # 1. Получение SQL от LLM
        sql_query = await get_sql_from_llm(modified_query)  # Используем измененный запрос
        if not sql_query or sql_query == "ERROR":
            await message.answer("Не удалось понять ваш запрос или сгенерировать SQL. Попробуйте переформулировать его.")
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
            await message.answer(f"{result}")
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
