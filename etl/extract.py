import requests
import pprint
import json
from datetime import datetime, timedelta, timezone

# получаем список товаров
products_url = "https://fakestoreapi.com/products"
resp_products = requests.get(products_url)
resp_products.raise_for_status()  # если статус не 200 — кинет ошибку
products = resp_products.json()  # преобразуем в json формат

# сохраняем сырые данные
with open("data/raw_products.json", "w", encoding="utf-8") as f:
    json.dump(products, f, ensure_ascii=False, indent=2)


# получаем официальный курс ЦБ РФ
resp = requests.get("https://www.cbr-xml-daily.ru/daily_json.js")
resp.raise_for_status()
cbr_data = resp.json()

with open("data/raw_cbr.json", "w", encoding="utf-8") as f:
    json.dump(cbr_data, f, ensure_ascii=False, indent=2)


# получаем погоду для Владивостока за последние 24 часа
now_utc = datetime.now(timezone.utc)
offset = timedelta(hours=10)
tz_vl = timezone(offset)
now_vl = now_utc + offset
start_dt = now_vl - timedelta(days=1)

weather_url = "https://api.open-meteo.com/v1/forecast"
weather_params = {
    "latitude": 43.1155,
    "longitude": 131.8855,
    "hourly": "temperature_2m",  # почасовая температура
    "start_date": start_dt.date().isoformat(),  # дата 24 часа назад
    "end_date": now_vl.date().isoformat(),       # сегодняшняя дата
    "timezone": "Asia/Vladivostok"
}
resp_weather = requests.get(weather_url, params=weather_params)
resp_weather.raise_for_status()
weather_data = resp_weather.json()

# сохраняем сырые данные по погоде
with open("data/raw_weather.json", "w", encoding="utf-8") as f:
    json.dump(weather_data, f, ensure_ascii=False, indent=2)

# фильтруем только последние 24 часа
times = weather_data.get("hourly", {}).get("time", [])
temps = weather_data.get("hourly", {}).get("temperature_2m", [])
last_24h = []
for t_str, temp in zip(times, temps):
    # превращает "2025-05-29T15:00" в datetime
    t = datetime.fromisoformat(t_str).replace(tzinfo=tz_vl)
    # добавляем только то время, которое попадает в сутки
    if start_dt <= t <= now_vl:
        last_24h.append((t_str, temp))


# получаем данные по крипте
url = "https://api.coingecko.com/api/v3/coins/markets"
params = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 10,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "24h"
}
resp = requests.get(url, params=params)
resp.raise_for_status()
crypto_data = resp.json()

# сохраняем сырые данные в файл
with open("data/raw_crypto.json", "w", encoding="utf-8") as f:
    json.dump(crypto_data, f, ensure_ascii=False, indent=2)
