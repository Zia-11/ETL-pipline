import requests
import pprint

# получаем список товаров
products_url = "https://fakestoreapi.com/products"
resp_products = requests.get(products_url)
resp_products.raise_for_status()  # если статус не 200 - кинет ошибку
products = resp_products.json()  # преобразуем в json формат
print("Пример товара:")
pprint.pprint(products[0])  # первый товар
