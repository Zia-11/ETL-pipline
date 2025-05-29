import requests
import pprint
from datetime import datetime, timedelta, timezone

# получаем список товаров
products_url = "https://fakestoreapi.com/products"
resp_products = requests.get(products_url)
resp_products.raise_for_status()  # если статус не 200 — кинет ошибку
products = resp_products.json()  # преобразуем в json формат
print("Пример товара:")
pprint.pprint(products[0])  # первый товар

# получаем курс доллара к рублю
rates_url = "https://open.er-api.com/v6/latest/USD"
resp_rates = requests.get(rates_url)
resp_rates.raise_for_status()
rate_data = resp_rates.json()

if rate_data.get("result") == "success" and "RUB" in rate_data.get("rates", {}):
    usd_to_rub = rate_data["rates"]["RUB"]
    print(f"\nКурс USD → RUB: {usd_to_rub}")
else:
    print("\nНе удалось получить курс валют. Ответ API:")
    pprint.pprint(rate_data)

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

print(
    f"\nТемпература во Владивостоке за последние 24 ч (с {start_dt.strftime('%Y-%m-%d %H:%M')} до {now_vl.strftime('%Y-%m-%d %H:%M')}):")
for t_str, temp in last_24h:
    print(f"{t_str} → {temp}°C")
