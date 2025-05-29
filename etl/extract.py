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
