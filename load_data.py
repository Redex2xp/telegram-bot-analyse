import asyncio
import json
import os
import uuid
from datetime import datetime

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

async def execute_sql_from_file(conn, file_path):
    """Функция выполняет SQL-скрипт из файла."""
    with open(file_path, 'r', encoding='utf-8') as f:
        await conn.execute(f.read())

async def load_data():
    """
    Функция для загрузки данных из JSON-файла в базу данных PostgreSQL.

    Выполняет следующие шаги:
    1. Устанавливает соединение с базой данных.
    2. Выполняет `init.sql` для создания таблиц и индексов.
    3. Читает данные из файла `videos.json`.
    4. Очищает таблицы перед вставкой новых данных.
    5. Вставляет данные в таблицы `videos` и `video_snapshots`.
    """
    conn = None
    try:
        conn = await asyncpg.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print("Соединение с базой данных установлено.")
        await execute_sql_from_file(conn, 'sql/init.sql')
        print("SQL-скрипт инициализации выполнен.")

        try:
            with open('videos.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            print("JSON-файл успешно прочитан.")
        except FileNotFoundError:
            print("Ошибка: Файл 'videos.json' не найден. Пожалуйста, убедитесь, что он находится в корневой директории проекта.")
            return
        except json.JSONDecodeError:
            print("Ошибка: Не удалось декодировать JSON. Проверьте формат файла.")
            return

        await conn.execute('TRUNCATE TABLE video_snapshots, videos RESTART IDENTITY CASCADE;')
        print("Таблицы очищены.")

        videos_to_insert = []
        snapshots_to_insert = []

        for video_data in data.get('videos', []):
            video_id = uuid.UUID(video_data['id'])
    
            videos_to_insert.append((
                video_id,
                uuid.UUID(video_data['creator_id']),
                datetime.fromisoformat(video_data['video_created_at']),
                video_data['views_count'],
                video_data['likes_count'],
                video_data['comments_count'],
                video_data['reports_count'],
                datetime.fromisoformat(video_data['created_at']),
                datetime.fromisoformat(video_data['updated_at'])
            ))

            for snapshot_data in video_data.get('snapshots', []):
                snapshots_to_insert.append((
                    uuid.UUID(snapshot_data['id']),
                    video_id,
                    snapshot_data['views_count'],
                    snapshot_data['likes_count'],
                    snapshot_data['comments_count'],
                    snapshot_data['reports_count'],
                    snapshot_data.get('delta_views_count', 0),
                    snapshot_data.get('delta_likes_count', 0),
                    snapshot_data.get('delta_comments_count', 0),
                    snapshot_data.get('delta_reports_count', 0),
                    datetime.fromisoformat(snapshot_data['created_at'])
                ))
        
        await conn.copy_records_to_table(
            'videos',
            records=videos_to_insert,
            columns=[
                'id', 'creator_id', 'video_created_at', 'views_count', 'likes_count',
                'comments_count', 'reports_count', 'created_at', 'updated_at'
            ]
        )
        
        await conn.copy_records_to_table(
            'video_snapshots',
            records=snapshots_to_insert,
            columns=[
                'id', 'video_id', 'views_count', 'likes_count', 'comments_count',
                'reports_count', 'delta_views_count', 'delta_likes_count',
                'delta_comments_count', 'delta_reports_count', 'created_at'
            ]
        )

        print(f"Данные успешно загружены: {len(videos_to_insert)} видео и {len(snapshots_to_insert)} снапшотов.")

    except asyncpg.exceptions.PostgresError as e:
        print(f"Ошибка при работе с PostgreSQL: {e}")
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
    finally:
        if conn:
            await conn.close()
            print("Соединение с базой данных закрыто.")

if __name__ == "__main__":
    asyncio.run(load_data())
