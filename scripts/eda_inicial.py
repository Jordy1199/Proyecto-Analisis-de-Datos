import polars as pl
import json
from pathlib import Path
from cargar_datos import cargar_datos

OUTPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"

def diagnostico(df: pl.DataFrame, nombre: str) -> dict:
    return {
        "nombre": nombre,
        "filas": df.height,
        "columnas": df.width,
        "tipos": {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)},
        "nulos": {col: int(df[col].null_count()) for col in df.columns},
        "duplicados": int(df.is_duplicated().sum()),
    }

def eda_inicial():
    train, stores, oil, holidays, transactions = cargar_datos()

    reporte = {
        "train": diagnostico(train, "train"),
        "stores": diagnostico(stores, "stores"),
        "oil": diagnostico(oil, "oil"),
        "holidays": diagnostico(holidays, "holidays"),
        "transactions": diagnostico(transactions, "transactions"),
    }

    reporte["train"]["rango_fechas"] = {
        "min": str(train["date"].min()),
        "max": str(train["date"].max()),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "eda_inicial.json", "w") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print(json.dumps(reporte, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    eda_inicial()
