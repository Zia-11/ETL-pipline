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


def get_or_create_product(conn, product_id, title, image, category_id):
    with conn.cursor() as cur:
        # проверка существует ли уже товар с таким product_id
        cur.execute(
            "SELECT product_id FROM dim_product WHERE product_id = %s;", (
                product_id,)
        )
        if cur.fetchone():
            # если есть — ничего не делаем, возвращаем product_id
            return product_id

        # иначе — вставляем новую запись в dim_product
        cur.execute(
            """
            INSERT INTO dim_product (product_id, title, image, category_id)
            VALUES (%s, %s, %s, %s);
            """,
            (product_id, title, image, category_id),
        )
        return product_id
