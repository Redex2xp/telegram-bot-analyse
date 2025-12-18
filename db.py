import os
import asyncpg
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

async def get_db_connection():
    """
    Функция устанавливает и возвращает асинхронное соединение с базой данных.
    
    Returns:
        asyncpg.Connection | None: Объект соединения или None в случае ошибки.
    """
    try:
        conn = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        logger.info("Установлено соединение с базой данных.")
        return conn
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Ошибка подключения к базе данных: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при подключении к БД: {e}", exc_info=True)
        return None

async def execute_query(conn: asyncpg.Connection, query: str):
    """
    Функция выполняет SQL-запрос и возвращает первый результат.
    Предназначена для запросов, возвращающих одно число.

    Args:
        conn (asyncpg.Connection): Активное соединение с БД.
        query (str): SQL-запрос для выполнения.

    Returns:
        Any | None: Результат запроса (одно значение) или None в случае ошибки.
    """
    try:
        result = await conn.fetchval(query)
        return result
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Ошибка выполнения SQL-запроса: {e}\nЗапрос: {query}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Непредвиденная ошибка при выполнении запроса: {e}", exc_info=True)
        return None
