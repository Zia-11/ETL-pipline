# load.py

import os
from dotenv import load_dotenv
import psycopg2
from transform import transform
from datetime import datetime

# загружаем переменные из .env
load_dotenv()

# читаем переменные окружения
DB_PARAMS = {
    "host":     os.getenv("PGHOST", "localhost"),
    "port":     os.getenv("PGPORT", "5432"),
    "dbname":   os.getenv("PGDATABASE", "mydb"),
    "user":     os.getenv("PGUSER", "myuser"),
    "password": os.getenv("PGPASSWORD", "mypassword"),
}


def get_or_create_category(conn, category_name):
    with conn.cursor() as cur:
        # сначала пытаемся найти существующую запись в таблице dim_category по имени категории
        cur.execute(
            "SELECT category_id FROM dim_category WHERE category_name = %s;",
            (category_name,),
        )
        row = cur.fetchone()
        if row:
            # если нашли (row не None), возвращаем уже существующий category_id
            return row[0]

        # если не нашли, вставляем новую строку (INSERT) и сразу возвращаем сгенерированный category_id
        cur.execute(
            "INSERT INTO dim_category (category_name) VALUES (%s) RETURNING category_id;",
            (category_name,),
        )
        return cur.fetchone()[0]
