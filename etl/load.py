# load.py

import os
from dotenv import load_dotenv
import psycopg2
from transform import transform
from datetime import datetime
from typing import Optional, Tuple

# вычисляем корень проекта, чтобы .env точно находился
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# загружаем переменные из .env, явно указав путь к файлу
dotenv_path = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path)


# читаем переменные окружения
def get_db_params() -> dict:
    params = {
        "host":     os.getenv("PGHOST"),
        "port":     os.getenv("PGPORT"),
        "dbname":   os.getenv("PGDATABASE"),
        "user":     os.getenv("PGUSER"),
        "password": os.getenv("PGPASSWORD"),
    }
    missing = [k for k, v in params.items() if v is None]
    if missing:
        print(
            f"[load] предупреждение: не найдены переменные окружения: {', '.join(missing)}")
    # возвращаем со стандартными значениями если что-то не задано
    return {
        "host":     params["host"] or "localhost",
        "port":     params["port"] or "5432",
        "dbname":   params["dbname"] or "mydb",
        "user":     params["user"] or "myuser",
        "password": params["password"] or "mypassword",
    }


DB_PARAMS = get_db_params()

LOCATION_NAME = "Vladivostok"
LATITUDE = 43.1155
LONGITUDE = 131.8855

CURRENCY_CODE = "USD"
CURRENCY_DESC = "US Dollar"

ASSET_ID = "bitcoin"
ASSET_SYMBOL = "btc"
ASSET_NAME = "Bitcoin"


# ФУНКЦИИ

def upsert_dimension(conn, select_sql: str, insert_sql: str, sel_params: tuple, ins_params: tuple) -> Optional[int]:
    # универсальная функция: если select_sql возвращает row - берёт row[0]
    # иначе выполняет insert_sql и возвращает сгенерированный id (row[0])
    with conn.cursor() as cur:
        cur.execute(select_sql, sel_params)
        row = cur.fetchone()
        if row:
            return row[0]
        cur.execute(insert_sql, ins_params)
        return cur.fetchone()[0]


def insert_fact_if_not_exists(conn, select_sql: str, insert_sql: str, sel_params: tuple, ins_params: tuple) -> None:
    # если select_sql не находит строку - выполняем insert_sql с ins_params
    with conn.cursor() as cur:
        cur.execute(select_sql, sel_params)
        if cur.fetchone() is None:
            cur.execute(insert_sql, ins_params)


def get_or_create_category(conn, category_name: str) -> Optional[int]:
    # получает category_id из dim_category по имени или создаёт новую запись
    select_sql = "SELECT category_id FROM dim_category WHERE category_name = %s;"
    insert_sql = "INSERT INTO dim_category (category_name) VALUES (%s) RETURNING category_id;"
    try:
        return upsert_dimension(conn, select_sql, insert_sql, (category_name,), (category_name,))
    except Exception as e:
        print(f"[get_or_create_category] ошибка: {e}")
        return None


def get_or_create_product(conn, product_id: int, title: str, image: str, category_id: int) -> Optional[int]:
    # получает или создаёт запись в dim_product / возвращает product_id или None при ошибке.
    select_sql = "SELECT product_id FROM dim_product WHERE product_id = %s;"
    insert_sql = """
        INSERT INTO dim_product (product_id, title, image, category_id)
        VALUES (%s, %s, %s, %s);
    """
    try:
        return upsert_dimension(conn, select_sql, insert_sql, (product_id,), (product_id, title, image, category_id))
    except Exception as e:
        print(f"[get_or_create_product] ошибка: {e}")
        return None


def get_or_create_time(conn, etl_time_str: str) -> Optional[int]:
    # получает или создаёт запись в dim_time
    # ожидается формат 'YYYY-MM-DD HH:MM:SS'
    try:
        dt = datetime.fromisoformat(etl_time_str)
    except ValueError:
        print(f"[get_or_create_time] неверный формат etl_time: {etl_time_str}")
        return None

    select_sql = "SELECT time_id FROM dim_time WHERE etl_time = %s;"
    insert_sql = """
        INSERT INTO dim_time (etl_time, date, hour, weekday)
        VALUES (%s, %s, %s, %s)
        RETURNING time_id;
    """
    date_ = dt.date()
    hour_ = dt.hour
    weekday_ = dt.isoweekday()

    try:
        return upsert_dimension(conn, select_sql, insert_sql, (dt,), (dt, date_, hour_, weekday_))
    except Exception as e:
        print(f"[get_or_create_time] ошибка: {e}")
        return None


def get_or_create_location(conn, location_name: str, latitude: float, longitude: float) -> Optional[int]:
    # получает или создаёт запись в dim_location
    select_sql = "SELECT location_id FROM dim_location WHERE location_name = %s;"
    insert_sql = """
        INSERT INTO dim_location (location_name, latitude, longitude)
        VALUES (%s, %s, %s)
        RETURNING location_id;
    """
    try:
        return upsert_dimension(conn, select_sql, insert_sql,
                                (location_name,), (location_name, latitude, longitude))
    except Exception as e:
        print(f"[get_or_create_location] ошибка: {e}")
        return None


def get_or_create_currency(conn, currency_code: str, description: Optional[str] = None) -> Optional[str]:
    # получает или создаёт валюту в dim_currency
    select_sql = "SELECT currency_code FROM dim_currency WHERE currency_code = %s;"
    insert_sql = "INSERT INTO dim_currency (currency_code, description) VALUES (%s, %s);"
    try:
        existing = upsert_dimension(
            conn, select_sql, insert_sql, (currency_code,), (currency_code, description))
        return existing if existing else currency_code
    except Exception as e:
        print(f"[get_or_create_currency] ошибка: {e}")
        return None


def get_or_create_crypto_asset(conn, asset_id: str, symbol: str, name: str) -> Optional[str]:
    # получает или создаёт запись в dim_crypto_asset
    select_sql = "SELECT asset_id FROM dim_crypto_asset WHERE asset_id = %s;"
    insert_sql = """
        INSERT INTO dim_crypto_asset (asset_id, symbol, name)
        VALUES (%s, %s, %s);
    """
    try:
        existing = upsert_dimension(
            conn, select_sql, insert_sql, (asset_id,), (asset_id, symbol, name))
        return existing if existing else asset_id
    except Exception as e:
        print(f"[get_or_create_crypto_asset] ошибка: {e}")
        return None


def load():
    # основной процесс загрузки: берём преобразованные записи из transform()
    # затем апсетим размерности и вставляем факты в соответствующие таблицы
    records = transform()
    if not records:
        print("[load] нет записей для загрузки, выходим.")
        return

    # берём первичный элемент чтобы получить общие параметры для всех записей
    first = records[0]
    etl_time_str = first.get("etl_time")
    cbr_rate = first.get("cbr_usd_rub")
    temp_snapshot = first.get("temp_snapshot")
    btc_price = first.get("btc_price_usd")
    btc_change = first.get("btc_change_24h")

    if not etl_time_str:
        print("[load] etl_time отсутствует в записи, выходим.")
        return

    # открываем соединение с базой
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            conn.autocommit = False  # начинаем транзакцию

            # проверяем соединение
            with conn.cursor() as test_cur:
                test_cur.execute("SELECT 1;")

            # dim_time
            time_id = get_or_create_time(conn, etl_time_str)
            if time_id is None:
                print("[load] не удалось получить time_id, откатываемся.")
                conn.rollback()
                return

            # dim_location
            location_id = get_or_create_location(
                conn, LOCATION_NAME, LATITUDE, LONGITUDE)
            if location_id is None:
                print("[load] не удалось получить location_id, откатываемся.")
                conn.rollback()
                return

            # dim_currency
            curr = get_or_create_currency(conn, CURRENCY_CODE, CURRENCY_DESC)
            if curr is None:
                print("[load] не удалось получить currency_code, откатываемся.")
                conn.rollback()
                return

            # dim_crypto_asset
            asset = get_or_create_crypto_asset(
                conn, ASSET_ID, ASSET_SYMBOL, ASSET_NAME)
            if asset is None:
                print("[load] не удалось получить asset_id, откатываемся.")
                conn.rollback()
                return

            # факты погоды
            select_weather = "SELECT fact_id FROM fact_weather WHERE time_id = %s AND location_id = %s;"
            insert_weather = """
                INSERT INTO fact_weather (time_id, location_id, temperature)
                VALUES (%s, %s, %s);
            """
            insert_fact_if_not_exists(conn,
                                      select_weather, insert_weather,
                                      (time_id, location_id), (time_id,
                                                               location_id, temp_snapshot)
                                      )

            # факты курса валют
            select_currency = "SELECT fact_id FROM fact_currency WHERE time_id = %s AND currency_code = %s;"
            insert_currency = """
                INSERT INTO fact_currency (time_id, currency_code, rate_cbr)
                VALUES (%s, %s, %s);
            """
            insert_fact_if_not_exists(conn,
                                      select_currency, insert_currency,
                                      (time_id, CURRENCY_CODE), (time_id,
                                                                 CURRENCY_CODE, cbr_rate)
                                      )

            # факты цены крипто
            select_crypto = "SELECT fact_id FROM fact_crypto_price WHERE time_id = %s AND asset_id = %s;"
            insert_crypto = """
                INSERT INTO fact_crypto_price (time_id, asset_id, price_usd, change_pct_24h)
                VALUES (%s, %s, %s, %s);
            """
            insert_fact_if_not_exists(conn,
                                      select_crypto, insert_crypto,
                                      (time_id, ASSET_ID), (time_id,
                                                            ASSET_ID, btc_price, btc_change)
                                      )

            # факты продаж (прогон по всем товарам)
            for rec in records:
                prod_id = rec.get("product_id")
                category = rec.get("category")
                price_usd = rec.get("price_usd")
                price_rub = rec.get("price_rub")
                sales = rec.get("sales")
                title = rec.get("title")
                image = rec.get("image")

                # dim_category
                category_id = get_or_create_category(conn, category)
                if category_id is None:
                    print(
                        f"[load] не удалось получить category_id для '{category}', пропускаем запись.")
                    continue

                # dim_product
                prod = get_or_create_product(
                    conn, prod_id, title, image, category_id)
                if prod is None:
                    print(
                        f"[load] не удалось получить product_id={prod_id}, пропускаем запись.")
                    continue

                # вставляем факт продаж
                select_sales = "SELECT fact_id FROM fact_sales WHERE product_id = %s AND time_id = %s;"
                insert_sales = """
                    INSERT INTO fact_sales (product_id, time_id, sales, price_usd, price_rub)
                    VALUES (%s, %s, %s, %s, %s);
                """
                insert_fact_if_not_exists(conn,
                                          select_sales, insert_sales,
                                          (prod_id, time_id), (prod_id,
                                                               time_id, sales, price_usd, price_rub)
                                          )

            # фиксируем транзакцию
            conn.commit()
            print("[load] загрузка в БД прошла успешно.")
    except psycopg2.OperationalError as e:
        print(f"[load] ошибка соединения с БД: {e}")
    except Exception as e:
        # при любой другой ошибке транзакция откатится
        print(f"[load] непредвиденная ошибка: {e}")
        raise


if __name__ == "__main__":
    load()
