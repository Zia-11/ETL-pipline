import requests
import json
import os
from datetime import datetime, timedelta, timezone


def fetch_products():
    # берем данные о товарах и сохраняем в файл
    url = "https://fakestoreapi.com/products"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    with open("data/raw_products.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data


def fetch_cbr_rate():
    # берем данные о курсе валюты и сохраняем в файл
    url = "https://www.cbr-xml-daily.ru/daily_json.js"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    with open("data/raw_cbr.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data["Valute"]["USD"]["Value"]


def fetch_weather():
    # берем данные о погоде и сохраняем в файл
    now_utc = datetime.now(timezone.utc)
    offset = timedelta(hours=10)
    now_vl = now_utc + offset
    date = now_vl.date().isoformat()
    params = {
        "latitude": 43.1155,
        "longitude": 131.8855,
        "hourly": "temperature_2m",
        "start_date": date,
        "end_date": date,
        "timezone": "Asia/Vladivostok"
    }
    url = "https://api.open-meteo.com/v1/forecast"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    with open("data/raw_weather.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # возвращаем последний час температуры
    temps = data["hourly"]["temperature_2m"]
    return temps[-1] if temps else None


def fetch_btc():
    # берем данные о биткоине и сохраняем в файл
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin",
        "sparkline": False,
        "price_change_percentage": "24h"
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    with open("data/raw_crypto.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # возвращаем первый элемент с нужными полями
    coin = data[0]
    return {
        "price": coin["current_price"],
        "change_24h": coin.get("price_change_percentage_24h", 0.0)
    }


if __name__ == "__main__":
    # тестовый прогон
    print(len(fetch_products()))
    print(fetch_cbr_rate())
    print(fetch_weather())
    print(fetch_btc())
