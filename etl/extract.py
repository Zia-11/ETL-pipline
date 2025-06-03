import requests
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any


# КОНСТАНТЫ

DATA_DIR = "data"

URL_PRODUCTS = "https://fakestoreapi.com/products"
URL_CBR_DAILY = "https://www.cbr-xml-daily.ru/daily_json.js"
URL_WEATHER = "https://api.open-meteo.com/v1/forecast"
URL_CRYPTO = "https://api.coingecko.com/api/v3/coins/markets"

WEATHER_LATITUDE = 43.1155
WEATHER_LONGITUDE = 131.8855
WEATHER_TIMEZONE = "Asia/Vladivostok"

# время ожидания запроса в секундах
REQUEST_TIMEOUT = 5


# ФУНКЦИИ

def fetch_products() -> List[Dict[str, Any]]:
    # загружает список товаров с fakestoreapi и сохраняет json в raw_products
    # возвращает список товарных словарей или пустой список если ошибка
    try:
        resp = requests.get(URL_PRODUCTS, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[fetch_products] Ошибка при запросе {URL_PRODUCTS}: {e}")
        return []

    data = resp.json()
    out_path = os.path.join(DATA_DIR, "raw_products.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return data


def fetch_cbr_rate() -> Optional[float]:
    # загружает json курсов валют и сохраняет в raw_cbr
    # возвращает курс доллара  или none если что то не так
    try:
        resp = requests.get(URL_CBR_DAILY, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[fetch_cbr_rate] Ошибка при запросе {URL_CBR_DAILY}: {e}")
        return None

    data = resp.json()
    out_path = os.path.join(DATA_DIR, "raw_cbr.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    try:
        return data["Valute"]["USD"]["Value"]
    except KeyError:
        print("[fetch_cbr_rate] Не найдено поле Valute→USD→Value в ответе")
        return None


def fetch_weather() -> Optional[float]:
    # запрашивает погоду (температуру по часам) для Владивостока на сегодня и сохраняет в raw_weather
    # возвращает температуру за последний час или None.
    try:
        now_vl = datetime.now(ZoneInfo(WEATHER_TIMEZONE))
    except Exception:
        from datetime import timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        offset = timedelta(hours=10)
        now_vl = now_utc + offset

    date_str = now_vl.date().isoformat()
    params = {
        "latitude": WEATHER_LATITUDE,
        "longitude": WEATHER_LONGITUDE,
        "hourly": "temperature_2m",
        "start_date": date_str,
        "end_date": date_str,
        "timezone": WEATHER_TIMEZONE
    }

    try:
        resp = requests.get(URL_WEATHER, params=params,
                            timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[fetch_weather] Ошибка при запросе {URL_WEATHER}: {e}")
        return None

    data = resp.json()

    out_path = os.path.join(DATA_DIR, "raw_weather.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    hourly = data.get("hourly", {})
    temps = hourly.get("temperature_2m", [])
    if not temps:
        print("[fetch_weather] Нет данных о температуре в ответе.")
        return None

    return temps[-1]


def fetch_btc() -> Optional[Dict[str, float]]:
    # загружает данные о биткоине (цена и изменение за 24h) и сохраняет в raw_crypto
    # возвращает словарь или none при ошибке
    params = {
        "vs_currency": "usd",
        "ids": "bitcoin",
        "sparkline": False,
        "price_change_percentage": "24h"
    }

    try:
        resp = requests.get(URL_CRYPTO, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"[fetch_btc] Ошибка при запросе {URL_CRYPTO}: {e}")
        return None

    data = resp.json()

    out_path = os.path.join(DATA_DIR, "raw_crypto.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    if not data:
        print("[fetch_btc] Ответ пустой.")
        return None

    coin = data[0]
    return {
        "price": coin.get("current_price", 0.0),
        "change_24h": coin.get("price_change_percentage_24h", 0.0)
    }


# ГЛАВНЫЙ БЛОК

if __name__ == "__main__":

    prods = fetch_products()
    print("Продуктов:", len(prods) if prods is not None else "Ошибка")

    rate = fetch_cbr_rate()
    print("Курс USD:", rate if rate is not None else "Ошибка")

    temp = fetch_weather()
    print("Температура (посл. час):", temp if temp is not None else "Нет данных")

    btc = fetch_btc()
    print("Bitcoin:", btc if btc is not None else "Ошибка")
