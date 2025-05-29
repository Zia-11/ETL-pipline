import requests
import pprint

# получаем список товаров
products_url = "https://fakestoreapi.com/products"
resp_products = requests.get(products_url)
resp_products.raise_for_status()  # если статус не 200 - кинет ошибку
products = resp_products.json()  # преобразуем в json формат
print("Пример товара:")
pprint.pprint(products[0])  # первый товар

# получаем курс доллара к рублю
rates_url = "https://open.er-api.com/v6/latest/USD"
resp_rates = requests.get(rates_url)
resp_rates.raise_for_status()
rate_data = resp_rates.json()

# проверка успешен ли запрос и есть ли курс рубля
if rate_data.get("result") == "success" and "RUB" in rate_data.get("rates", {}):
    usd_to_rub = rate_data["rates"]["RUB"]
    print(f"\nКурс USD → RUB: {usd_to_rub}")
else:
    print("\nНе удалось получить курс валют. Ответ API:")
    pprint.pprint(rate_data)

# получаем погоду для Владивостока
weather_url = "https://api.open-meteo.com/v1/forecast"
weather_params = {
    "latitude": 43.1155,
    "longitude": 131.8855,
    "hourly": "temperature_2m",
    "start_date": "2025-05-20",
    "end_date": "2025-05-21",
    "timezone": "Asia/Vladivostok"
}
resp_weather = requests.get(weather_url, params=weather_params)
resp_weather.raise_for_status()
weather_data = resp_weather.json()

temps = weather_data.get("hourly", {}).get("temperature_2m", [])
print(f"\nПервые 5 значений температуры во Владивостоке: {temps[:5]}")
