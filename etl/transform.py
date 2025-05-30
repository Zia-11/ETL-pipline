import json
from datetime import datetime, timezone


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
    etl_time = datetime.now(timezone.utc).isoformat()

    # извлекаем поля с товаров
    records = []
    for p in products:
        prod_id = p.get("id")
        category = p.get("category")
        price_usd = float(p.get("price", 0))
        sales = p.get("rating", {}).get("count", 0)  # прокси-продажи
        if sales <= 0:
            continue

        # пересчитываем цену в рубли
        price_rub = round(price_usd * cbr_rate, 2) if cbr_rate else None

        # добавляем все нужные поля в словарь
        record = {
            "product_id":    prod_id,
            "category":      category,
            "sales":         sales,
            "price_usd":     price_usd,
            "price_rub":     price_rub,
            "cbr_usd_rub":   cbr_rate,
            "temp_snapshot": temp_snapshot,
            "btc_price_usd": crypto.get("current_price"),
            "btc_change_24h": crypto.get("price_change_percentage_24h"),
            "etl_time":      etl_time
        }
        records.append(record)

    return records
