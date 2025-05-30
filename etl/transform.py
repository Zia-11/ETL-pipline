import json


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
