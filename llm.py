import os
import httpx
from dotenv import load_dotenv
import logging
import asyncio

load_dotenv()

logger = logging.getLogger(__name__)

# Загрузка ключа API из переменных окружения
AGENTPLATFORM_KEY = os.getenv("AGENTPLATFORM_KEY")

PROMPT_TEMPLATE = """
Ты — продвинутый AI-ассистент, который преобразует запросы на естественном языке в SQL-запросы для базы данных PostgreSQL.
Твоя задача — сгенерировать ТОЛЬКО ОДИН SQL-запрос, который точно соответствует вопросу пользователя. НЕ добавляй никаких пояснений, комментариев или дополнительного текста. Только SQL.

Схема базы данных:
{schema}

Несколько правил и примеров:

1.  **Всегда возвращай только SQL.**
    -   Пример вопроса: "Сколько всего видео есть в системе?"
    -   Правильный ответ: SELECT COUNT(*) FROM videos;

2.  **Обрабатывай диапазоны дат.** Даты в запросах могут быть в разных форматах ("1 ноября 2025", "с 1 по 5 ноября 2025").
    -   `video_created_at` в таблице `videos` — это дата публикации видео.
    -   `created_at` в таблице `video_snapshots` — это время замера.
    -   Пример вопроса: "Сколько видео у креатора с id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
    -   Правильный ответ: SELECT COUNT(*) FROM videos WHERE creator_id = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee' AND video_created_at BETWEEN '2025-11-01' AND '2025-11-05 23:59:59';

3.  **Считай прирост по дельтам.** Для вопросов о приросте используй `delta_*` поля в `video_snapshots`.
    -   Пример вопроса: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
    -   Правильный ответ: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

4.  **Считай уникальные сущности.** Для вопросов о "разных" или "уникальных" видео используй `COUNT(DISTINCT ...)`.
    -   Пример вопроса: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
    -   Правильный ответ: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;
    
5.  **Фильтруй по финальным значениям.** Для вопросов типа "больше X просмотров" используй итоговые счётчики в таблице `videos`.
    -   Пример вопроса: "Сколько видео набрало больше 100000 просмотров за всё время?"
    -   Правильный ответ: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Теперь, основываясь на этой схеме и примерах, преобразуй следующий запрос. Помни: только SQL-код.

Запрос пользователя: "{user_query}"

Если не можешь сгенерировать корректный запрос, верни одно слово: ERROR.
"""

def get_schema():
    """Функция читает схему из SQL-файла."""
    try:
        with open('sql/init.sql', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("Файл схемы 'sql/init.sql' не найден.")
        return ""

async def get_sql_from_llm(user_query: str, max_retries: int = 3, initial_backoff: float = 5.0) -> str | None:
    """
    Функция отправляет запрос к LLM API для преобразования текста в SQL.
    Использует httpx для асинхронных запросов и включает механизм повторных попыток.

    Args:
        user_query (str): Запрос пользователя на естественном языке.
        max_retries (int): Максимальное количество повторных попыток.
        initial_backoff (float): Начальное время ожидания в секундах.

    Returns:
        str | None: Сгенерированный SQL-запрос или None в случае ошибки.
    """
    if not AGENTPLATFORM_KEY:
        logger.error("Ключ API AGENTPLATFORM_KEY не найден. Проверьте .env файл.")
        return None

    schema = get_schema()
    if not schema:
        return None
    
    prompt = PROMPT_TEMPLATE.format(schema=schema, user_query=user_query)
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {AGENTPLATFORM_KEY}"
    }
    
    data = {
        "model": "openai/gpt-4o",
        "messages": [{"role": "user", "content": prompt}]
    }
    
    url = "https://litellm.tokengate.ru/v1/chat/completions"
    
    attempt = 0
    backoff = initial_backoff
    async with httpx.AsyncClient(timeout=60.0) as client:
        while attempt <= max_retries:
            try:
                response = await client.post(url, headers=headers, json=data)
                response.raise_for_status()  # Вызовет исключение для статусов 4xx/5xx
                
                result = response.json()
                sql_query = result['choices'][0]['message']['content'].strip()

                if sql_query.lower().startswith("```sql"):
                    sql_query = sql_query[5:]
                if sql_query.endswith("```"):
                    sql_query = sql_query[:-3]
                if sql_query.endswith(';'):
                    sql_query = sql_query[:-1]

                return sql_query.strip()

            except httpx.HTTPStatusError as e:
                logger.error(f"Ошибка API (статус {e.response.status_code}): {e.response.text}", exc_info=True)
                if e.response.status_code in [429, 500, 502, 503, 504]: # Ошибки, при которых стоит повторить
                    if attempt == max_retries:
                        logger.error("Достигнуто максимальное количество повторных попыток.")
                        return None
                    
                    logger.warning(f"Попытка {attempt + 1} из {max_retries}. Ожидание {backoff:.2f} сек.")
                    await asyncio.sleep(backoff)
                    attempt += 1
                    backoff *= 2
                else: # Нерешаемые ошибки клиента
                    return None

            except Exception as e:
                logger.error(f"Непредвиденная ошибка при взаимодействии с API: {e}", exc_info=True)
                return None

    return None
