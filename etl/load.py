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


def get_or_create_time(conn, etl_time_str):
    dt = datetime.fromisoformat(etl_time_str)
    with conn.cursor() as cur:
        # существует ли время с таким exact etl_time
        cur.execute(
            "SELECT time_id FROM dim_time WHERE etl_time = %s;", (dt,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # если не нашли — готовим все колонки для вставки в dim_time
        date_ = dt.date()
        hour_ = dt.hour
        weekday_ = dt.isoweekday()  # 1–7 (понедельник–воскресенье)

        # вставляем новую запись и возвращаем сгенерированный SERIAL time_id
        cur.execute(
            """
            INSERT INTO dim_time (etl_time, date, hour, weekday)
            VALUES (%s, %s, %s, %s)
            RETURNING time_id;
            """,
            (dt, date_, hour_, weekday_),
        )
        return cur.fetchone()[0]


def get_or_create_location(conn, location_name, latitude, longitude):
    with conn.cursor() as cur:
        # есть ли локация с данным именем
        cur.execute(
            "SELECT location_id FROM dim_location WHERE location_name = %s;", (
                location_name,)
        )
        row = cur.fetchone()
        if row:
            return row[0]

        # если не нашли – вставляем новую запись с названием, широтой и долготой
        cur.execute(
            """
            INSERT INTO dim_location (location_name, latitude, longitude)
            VALUES (%s, %s, %s)
            RETURNING location_id;
            """,
            (location_name, latitude, longitude),
        )
        return cur.fetchone()[0]


def get_or_create_currency(conn, currency_code, description=None):
    with conn.cursor() as cur:
        # существует ли уже валюта с данным кодом
        cur.execute(
            "SELECT currency_code FROM dim_currency WHERE currency_code = %s;", (
                currency_code,)
        )
        if cur.fetchone():
            return currency_code

        # если нет — вставляем
        cur.execute(
            "INSERT INTO dim_currency (currency_code, description) VALUES (%s, %s);",
            (currency_code, description),
        )
        return currency_code


def get_or_create_crypto_asset(conn, asset_id, symbol, name):
    with conn.cursor() as cur:
        # есть ли уже такой asset_id
        cur.execute(
            "SELECT asset_id FROM dim_crypto_asset WHERE asset_id = %s;", (
                asset_id,)
        )
        if cur.fetchone():
            return asset_id

        # если нет — вставляем новую запись с asset_id, symbol и name
        cur.execute(
            """
            INSERT INTO dim_crypto_asset (asset_id, symbol, name)
            VALUES (%s, %s, %s);
            """,
            (asset_id, symbol, name),
        )
        return asset_id
