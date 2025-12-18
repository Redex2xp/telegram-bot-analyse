import os
import google.generativeai as genai
from dotenv import load_dotenv
import logging

load_dotenv()


logger = logging.getLogger(__name__)


GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


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
"""

def get_schema():
    """Функция читает схему из SQL-файла."""
    try:
        with open('sql/init.sql', 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error("Файл схемы 'sql/init.sql' не найден.")
        return ""

async def get_sql_from_llm(user_query: str) -> str | None:
    """
    Функция отправляет запрос к Gemini для преобразования текста в SQL.

    Args:
        user_query (str): Запрос пользователя на естественном языке.

    Returns:
        str | None: Сгенерированный SQL-запрос или None в случае ошибки.
    """
    if not GEMINI_API_KEY:
        logger.error("API-ключ для Gemini не найден. Проверьте .env файл.")
        return None

    try:
        model = genai.GenerativeModel('gemini-2.5-pro')
        
        schema = get_schema()
        if not schema:
            return None
            
        prompt = PROMPT_TEMPLATE.format(schema=schema, user_query=user_query)
        
        response = await model.generate_content_async(prompt)
        
        sql_query = response.text.strip()
        if sql_query.lower().startswith("```sql"):
            sql_query = sql_query[5:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        if sql_query.endswith(';'):
            sql_query = sql_query[:-1]

        return sql_query.strip()

    except Exception as e:
        logger.error(f"Ошибка при взаимодействии с Gemini API: {e}", exc_info=True)
        return None
