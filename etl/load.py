import logging
import os
from transform import transform
from datetime import datetime

# настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


#  параметры подключения
DSN = os.getenv(
    "ETL_DSN", "dbname=etl_pipline user=user password=root host=localhost port=5432")


def load_etl():
    # функция загрузки: берем данные с transform и заполняем таблицы
    logging.info("Запускаем этап LOAD...")
    records = transform()
    if not records:
        logging.warning("Transform вернул пустой список. Нечего загружать.")
        return

    # парсим etl_time
    etl_time_str = records[0]["etl_time"]
    try:
        etl_time = datetime.strptime(etl_time_str, "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error("Неправильный формат etl_time: %s", etl_time_str)
        raise
