from etl.transform import transform
import pandas as pd

# получаем данные
records = transform()

# превращаем в DataFrame
df = pd.DataFrame(records)

# смотрим первые строки и типы колонок
print(df.head(10))  # первые 10 записей
print("\nТипы колонок:\n", df.dtypes)

# сохраняем в CSV
df.to_csv("data/transformed.csv", index=False, encoding="utf-8")
print("\nСохранено в data/transformed.csv")
