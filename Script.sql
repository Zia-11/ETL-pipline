-- размерности

CREATE TABLE IF NOT EXISTS dim_category (
  category_id   INTEGER PRIMARY KEY,
  category_name TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
  product_id  INTEGER PRIMARY KEY,
  title       TEXT    NOT NULL,
  image       TEXT,
  category_id INTEGER NOT NULL
    REFERENCES dim_category(category_id)
);

CREATE TABLE dim_time (
  time_id   SERIAL PRIMARY KEY,
  etl_time  TIMESTAMP    NOT NULL UNIQUE,  
  date      DATE         NOT NULL,
  hour      INTEGER      NOT NULL,    
  weekday   INTEGER      NOT NULL 
);

CREATE TABLE dim_location (
  location_id   SERIAL PRIMARY KEY,
  location_name TEXT    NOT NULL,
  latitude      DOUBLE PRECISION NOT NULL,
  longitude     DOUBLE PRECISION NOT NULL
);


CREATE TABLE dim_currency (
  currency_code TEXT    PRIMARY KEY,
  description   TEXT
);

CREATE TABLE dim_crypto_asset (
  asset_id TEXT PRIMARY KEY,
  symbol   TEXT NOT NULL,
  name     TEXT NOT NULL
);

-- факты

CREATE TABLE fact_sales (
  fact_id     SERIAL PRIMARY KEY,
  product_id  INTEGER NOT NULL
    REFERENCES dim_product(product_id),
  time_id     INTEGER NOT NULL
    REFERENCES dim_time(time_id),
  sales       INTEGER,
  price_usd   DOUBLE PRECISION,
  price_rub   DOUBLE PRECISION
);

CREATE TABLE fact_weather (
  fact_id     SERIAL PRIMARY KEY,
  time_id     INTEGER NOT NULL
    REFERENCES dim_time(time_id),
  location_id INTEGER NOT NULL
    REFERENCES dim_location(location_id),
  temperature DOUBLE PRECISION
);

CREATE TABLE fact_currency (
  fact_id       SERIAL PRIMARY KEY,
  time_id       INTEGER NOT NULL
    REFERENCES dim_time(time_id),
  currency_code TEXT NOT NULL
    REFERENCES dim_currency(currency_code),
  rate_cbr      DOUBLE PRECISION
);

CREATE TABLE fact_crypto_price (
  fact_id        SERIAL PRIMARY KEY,
  time_id        INTEGER NOT NULL
    REFERENCES dim_time(time_id),
  asset_id       TEXT NOT NULL
    REFERENCES dim_crypto_asset(asset_id),
  price_usd      DOUBLE PRECISION,
  change_pct_24h DOUBLE PRECISION
);