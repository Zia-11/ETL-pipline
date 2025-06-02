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


def load():
    # получаем уже преобразованные записи из transform()
    records = transform()
    if not records:
        print("Нет записей для загрузки.")
        return

    # извлекаем «единые» данные из первого элемента списка records
    first = records[0]
    etl_time_str = first["etl_time"]      # строка вида "YYYY-MM-DD HH:MM:SS"
    cbr_rate = first["cbr_usd_rub"]   # курс USD - RUB
    temp_snapshot = first["temp_snapshot"]  # температура последнего часа
    btc_price = first["btc_price_usd"]  # текущая цена BTC в USD
    btc_change = first["btc_change_24h"]  # изменение BTC за 24ч в процентах

    # статические «размеры» (они одни и те же для всех записей)
    LOCATION_NAME = "Vladivostok"
    LATITUDE = 43.1155
    LONGITUDE = 131.8855

    CURRENCY_CODE = "USD"
    CURRENCY_DESC = "US Dollar"

    ASSET_ID = "bitcoin"
    ASSET_SYMBOL = "btc"
    ASSET_NAME = "Bitcoin"

    # открываем соединение с базой, используя DB_PARAMS
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        conn.autocommit = False  # начинаем транзакцию

        # Upsert в dim_time: получаем или создаём запись для текущего etl_time
        time_id = get_or_create_time(conn, etl_time_str)

        # Upsert в dim_location: получаем или создаём запись для "Vladivostok"
        location_id = get_or_create_location(
            conn, LOCATION_NAME, LATITUDE, LONGITUDE)

        # Upsert в dim_currency: получаем или создаём запись для USD
        get_or_create_currency(conn, CURRENCY_CODE, CURRENCY_DESC)

        # Upsert в dim_crypto_asset: получаем или создаём запись для Bitcoin
        get_or_create_crypto_asset(conn, ASSET_ID, ASSET_SYMBOL, ASSET_NAME)

        # Вставляем факт погоды (fact_weather) — только если ещё нет записи для этой time_id и location_id
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fact_id
                FROM fact_weather
                WHERE time_id = %s AND location_id = %s;
                """,
                (time_id, location_id),
            )
            if cur.fetchone() is None:
                # если записи нет, вставляем
                cur.execute(
                    """
                    INSERT INTO fact_weather (time_id, location_id, temperature)
                    VALUES (%s, %s, %s);
                    """,
                    (time_id, location_id, temp_snapshot),
                )

        # вставляем факт курса валют (fact_currency) — если ещё нет записи для этой time_id и USD
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fact_id
                FROM fact_currency
                WHERE time_id = %s AND currency_code = %s;
                """,
                (time_id, CURRENCY_CODE),
            )
            if cur.fetchone() is None:
                cur.execute(
                    """
                    INSERT INTO fact_currency (time_id, currency_code, rate_cbr)
                    VALUES (%s, %s, %s);
                    """,
                    (time_id, CURRENCY_CODE, cbr_rate),
                )

        # вставляем факт цены крипто (fact_crypto_price) — если ещё нет для этой time_id и asset_id
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT fact_id
                FROM fact_crypto_price
                WHERE time_id = %s AND asset_id = %s;
                """,
                (time_id, ASSET_ID),
            )
            if cur.fetchone() is None:
                cur.execute(
                    """
                    INSERT INTO fact_crypto_price (time_id, asset_id, price_usd, change_pct_24h)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (time_id, ASSET_ID, btc_price, btc_change),
                )

        # цикл по каждому record (каждому товару) для загрузки данных продаж
        for rec in records:
            prod_id = rec["product_id"]    # ID товара из API
            category = rec["category"]      # Название категории
            price_usd = rec["price_usd"]    # Цена USD
            price_rub = rec["price_rub"]    # Цена RUB (рассчитанная)
            sales = rec["sales"]         # Количество продаж (proxy)
            title = rec.get("title")     # Название товара
            image = rec.get("image")     # URL картинки товара

            # получаем или создаём категорию
            category_id = get_or_create_category(conn, category)

            # получаем или создаём товар с его title/image и category_id
            get_or_create_product(conn, prod_id, title, image, category_id)

            # вставляем факт продаж (если ещё нет) для данного product_id и time_id
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT fact_id
                    FROM fact_sales
                    WHERE product_id = %s AND time_id = %s;
                    """,
                    (prod_id, time_id),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        """
                        INSERT INTO fact_sales (product_id, time_id, sales, price_usd, price_rub)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (prod_id, time_id, sales, price_usd, price_rub),
                    )

        # если всё прошло без ошибок, фиксируем транзакцию
        conn.commit()
        print("Загрузка в БД прошла успешно.")
    except Exception as e:
        # при любой ошибке делаем откат и печатаем сообщение
        conn.rollback()
        print("Ошибка при загрузке:", e)
        raise
    finally:
        # закрываем соединение в любом случае
        conn.close()


if __name__ == "__main__":
    load()
