import polars as pl
from pathlib import Path

RAW_DIR = Path.home() / "favorita_pipeline" / "data" / "raw" / "store-sales-time-series-forecasting"

def cargar_datos():
    train = pl.read_csv(RAW_DIR / "train.csv", try_parse_dates=True)
    stores = pl.read_csv(RAW_DIR / "stores.csv")
    oil = pl.read_csv(RAW_DIR / "oil.csv", try_parse_dates=True)
    holidays = pl.read_csv(RAW_DIR / "holidays_events.csv", try_parse_dates=True)
    transactions = pl.read_csv(RAW_DIR / "transactions.csv", try_parse_dates=True)

    print("train:", train.shape)
    print("stores:", stores.shape)
    print("oil:", oil.shape)
    print("holidays:", holidays.shape)
    print("transactions:", transactions.shape)

    return train, stores, oil, holidays, transactions

if __name__ == "__main__":
    cargar_datos()
