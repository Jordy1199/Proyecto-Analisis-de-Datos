import polars as pl
from pathlib import Path

INPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"
OUTPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"


def consolidar():
    train = pl.read_parquet(INPUT_DIR / "train_limpio.parquet")
    stores = pl.read_parquet(INPUT_DIR / "stores_limpio.parquet")
    oil = pl.read_parquet(INPUT_DIR / "oil_limpio.parquet")
    holidays = pl.read_parquet(INPUT_DIR / "holidays_limpio.parquet")
    transactions = pl.read_parquet(INPUT_DIR / "transactions_limpio.parquet")

    print("Filas antes de consolidar:")
    print("train:", train.shape)
    print("stores:", stores.shape)
    print("oil:", oil.shape)
    print("holidays:", holidays.shape)
    print("transactions:", transactions.shape)
    print()

    df = train.join(stores, on="store_nbr", how="left")
    df = df.join(transactions, on=["store_nbr", "date"], how="left")
    df = df.join(oil, on="date", how="left")
    df = df.join(holidays, on="date", how="left")

    print("Consolidado final:", df.shape)
    print("Columnas:", df.columns)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUTPUT_DIR / "consolidado.parquet")

    print()
    print("Archivo consolidado guardado en", OUTPUT_DIR / "consolidado.parquet")

    return df


if __name__ == "__main__":
    consolidar()
