import json
import os
import random
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List, Dict, Any

# вычисляем пути к файлам относительно корня проекта
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.path.join(PROJECT_DIR, "data")

# КОНСТАНТЫ

RAW_PRODUCTS = os.path.join(DATA_DIR, "raw_products.json")
RAW_CBR = os.path.join(DATA_DIR, "raw_cbr.json")
RAW_WEATHER = os.path.join(DATA_DIR, "raw_weather.json")
RAW_CRYPTO = os.path.join(DATA_DIR, "raw_crypto.json")
SALES_HISTORY = os.path.join(DATA_DIR, "sales_history.json")


# ФУНКЦИИ

def load_json(path: str) -> Any:
    # загружает json из файла path, или возвращает none и предупреждение если не удалось
    if not os.path.exists(path):
        print(f"[load_json] файл {path} не найден")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"[load_json] ошибка: не удалось декодировать {path} как json")
        return None


def build_record(
    p: Dict[str, Any],
    cbr_rate: Optional[float],
    temp_snapshot: Optional[float],
    crypto: Dict[str, Any],
    etl_time: str,
    sales_history: Dict[str, int]
) -> Dict[str, Any]:
    # собирает один словарь для товара p
    # обновляет sales_history в памяти (не сохраняет в файл)
    # возвращает готовый к загрузке словарь
    prod_id = p.get("id")
    title = p.get("title")
    image = p.get("image")
    category = p.get("category")
    try:
        base_price_usd = float(p.get("price", 0))
    except (ValueError, TypeError):
        base_price_usd = 0.0
        print(
            f"[build_record] неверная цена у товара id={prod_id}, установлено 0.0")

    base_sales = p.get("rating", {}).get("count", 0)

    # получаем предыдущее значение продаж
    prev_sales = sales_history.get(str(prod_id), base_sales)

    # генерируем случайное увеличение
    increment = random.randint(1, 10)
    new_sales = prev_sales + increment

    # сохраняем в историю
    sales_history[str(prod_id)] = new_sales

    # пересчитываем цену в рубли если курс есть
    price_rub = round(base_price_usd * cbr_rate, 2) if cbr_rate else None

    # собираем итоговый record
    record = {
        "product_id":      prod_id,
        "title":           title,
        "image":           image,
        "category":        category,
        "sales":           new_sales,
        "price_usd":       base_price_usd,
        "price_rub":       price_rub,
        "cbr_usd_rub":     cbr_rate,
        "temp_snapshot":   temp_snapshot,
        "btc_price_usd":   crypto.get("current_price") if crypto else None,
        "btc_change_24h":  crypto.get("price_change_percentage_24h") if crypto else None,
        "etl_time":        etl_time
    }
    return record


def transform() -> List[Dict[str, Any]]:
    # читает сырые данные и обновляет историю продаж в sales_history
    # возвращает список записей готовых для загрузки
    products = load_json(RAW_PRODUCTS)
    raw_cbr = load_json(RAW_CBR)
    raw_weather = load_json(RAW_WEATHER)
    raw_crypto_list = load_json(RAW_CRYPTO)

    # если хоть один из необходимых файлов не загрузился - завершаем трансформацию
    if products is None or raw_cbr is None or raw_weather is None or raw_crypto_list is None:
        print("[transform] недостаточно данных для трансформации, выходим.")
        return []

    # распаковываем нужные значения
    cbr_rate = raw_cbr.get("Valute", {}).get("USD", {}).get("Value")
    if cbr_rate is None:
        print("[transform] не удалось найти курс USD в cbr-данных")

    hourly = raw_weather.get("hourly", {})
    temps = hourly.get("temperature_2m", [])
    temp_snapshot = temps[-1] if temps else None
    if temp_snapshot is None:
        print("[transform] нет данных о температуре за последний час")

    if isinstance(raw_crypto_list, list) and raw_crypto_list:
        crypto = raw_crypto_list[0]
    else:
        crypto = {}
        print("[transform] raw_crypto.json не содержит элементов")

    # получаем текущее время в зоне Владивостока
    try:
        now_vl = datetime.now(ZoneInfo("Asia/Vladivostok"))
    except Exception:
        from datetime import timezone, timedelta
        now_utc = datetime.now(timezone.utc)
        now_vl = now_utc + timedelta(hours=10)
    etl_time = now_vl.strftime("%Y-%m-%d %H:%M:%S")

    # загружаем историю продаж
    if os.path.exists(SALES_HISTORY):
        history_data = load_json(SALES_HISTORY)
        sales_history = history_data if isinstance(history_data, dict) else {}
        if not isinstance(history_data, dict):
            print("[transform] sales_history.json не dict, используем пустой словарь")
    else:
        sales_history = {}

    # формируем новые записи
    records = []
    for p in products:
        rec = build_record(p, cbr_rate, temp_snapshot,
                           crypto, etl_time, sales_history)
        records.append(rec)

    # сохраняем обновлённый sales_history
    temp_path = SALES_HISTORY + ".tmp"
    try:
        with open(temp_path, "w", encoding="utf-8") as hist_f:
            json.dump(sales_history, hist_f, ensure_ascii=False, indent=2)
        os.replace(temp_path, SALES_HISTORY)
    except IOError as e:
        print(f"[transform] ошибка записи истории продаж: {e}")

    return records


if __name__ == "__main__":
    example = transform()
    print("примеры первых трёх записей:", example[:3])
