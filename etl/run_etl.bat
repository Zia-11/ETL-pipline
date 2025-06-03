@echo off

cd /d "C:\Users\Huawei\OneDrive\Desktop\kursovaya\ETL_pipline\etl"

python extract.py
python transform.py
python load.py
