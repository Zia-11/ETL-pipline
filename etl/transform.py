# transform.py

import json
import os
import random
from datetime import datetime, timezone, timedelta


def transform():
    # загрузка сырых данных
    with open("data/raw_products.json", "r", encoding="utf-8") as f:
        products = json.load(f)
    with open("data/raw_cbr.json", "r", encoding="utf-8") as f:
        cbr_rate = json.load(f).get("Valute", {}).get("USD", {}).get("Value")
    with open("data/raw_weather.json", "r", encoding="utf-8") as f:
        weather = json.load(f).get("hourly", {})
    with open("data/raw_crypto.json", "r", encoding="utf-8") as f:
        crypto = json.load(f)[0]

    # вычисляем температуру последнего часа
    temps = weather.get("temperature_2m", [])
    temp_snapshot = temps[-1] if temps else None

    # отметка времени запуска ETL
    tz_vl = timezone(timedelta(hours=10))
    etl_time = datetime.now(tz_vl).strftime("%Y-%m-%d %H:%M:%S")

    # путь к файлу истории продаж
    history_path = "data/sales_history.json"
    if os.path.exists(history_path):
        with open(history_path, "r", encoding="utf-8") as hist_f:
            sales_history = json.load(hist_f)
    else:
        sales_history = {}

    records = []
    for p in products:
        prod_id = p.get("id")
        title = p.get("title")
        image = p.get("image")
        category = p.get("category")
        base_price_usd = float(p.get("price", 0))
        base_sales = p.get("rating", {}).get("count", 0)

        # получаем предыдущее значение продаж для этого товара
        prev_sales = sales_history.get(str(prod_id), base_sales)
        # генерируем увеличение (от 1 до 10)
        increment = random.randint(1, 10)
        new_sales = prev_sales + increment

        # сохраняем обновлённое значение в историю
        sales_history[str(prod_id)] = new_sales

        # пересчитываем цену в рубли
        price_rub = round(base_price_usd * cbr_rate, 2) if cbr_rate else None

        record = {
            "product_id":    prod_id,
            "title":         title,
            "image":         image,
            "category":      category,
            "sales":         new_sales,
            "price_usd":     base_price_usd,
            "price_rub":     price_rub,
            "cbr_usd_rub":   cbr_rate,
            "temp_snapshot": temp_snapshot,
            "btc_price_usd": crypto.get("current_price"),
            "btc_change_24h": crypto.get("price_change_percentage_24h"),
            "etl_time":      etl_time
        }
        records.append(record)

    # сохраняем историю продаж обратно
    with open(history_path, "w", encoding="utf-8") as hist_f:
        json.dump(sales_history, hist_f, ensure_ascii=False, indent=2)

    return records


if __name__ == "__main__":
    example = transform()
    print(example[:3])
