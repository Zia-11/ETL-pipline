import logging
import os

# настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


#  параметры подключения
DSN = os.getenv(
    "ETL_DSN", "dbname=etl_pipline user=user password=root host=localhost port=5432")
