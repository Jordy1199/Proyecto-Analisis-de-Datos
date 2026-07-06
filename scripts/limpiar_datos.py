import polars as pl
from pathlib import Path
from cargar_datos import cargar_datos



#Ruta de salida
OUTPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"
#Funcion principal

def limpiar_datos():
    train, stores, oil, holidays, transactions = cargar_datos()

    # Interpolacion de valores nulos en la columna dcoilwtico del dataframe oil
    oil = oil.sort("date")
    #Interpolar con valor intermedio entre los valores anteriores y posteriores
    oil = oil.with_columns(pl.col("dcoilwtico").interpolate())
    #Llenar el primer valor nulo con el anterior o posterior valor válido
    oil = oil.with_columns(
        pl.col("dcoilwtico").fill_null(strategy="forward").fill_null(strategy="backward")
    )


    #Nulos por columna en cada tabla
    print("Nulos por columna en cada tabla:")
    print("train:", {col: train[col].null_count() for col in train.columns})
    print("stores:", {col: stores[col].null_count() for col in stores.columns})
    print("oil:", {col: oil[col].null_count() for col in oil.columns})
    print("holidays:", {col: holidays[col].null_count() for col in holidays.columns})
    print("transactions:", {col: transactions[col].null_count() for col in transactions.columns})

    print()




    #Eliminar duplicados (Si los hay) en cada uno de los dataframes
    train = train.unique()
    stores = stores.unique()
    oil = oil.unique()
    holidays = holidays.unique()
    transactions = transactions.unique()

    print("Despues de quitar duplicados:")
    print()
    print("train:", train.shape)
    print("stores:", stores.shape)
    print("oil:", oil.shape)
    print("holidays:", holidays.shape)
    print("transactions:", transactions.shape)

    #Crear las carpeta si no existen
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    train.write_parquet(OUTPUT_DIR / "train_limpio.parquet")
    stores.write_parquet(OUTPUT_DIR / "stores_limpio.parquet")
    oil.write_parquet(OUTPUT_DIR / "oil_limpio.parquet")
    holidays.write_parquet(OUTPUT_DIR / "holidays_limpio.parquet")
    transactions.write_parquet(OUTPUT_DIR / "transactions_limpio.parquet")

    print()
    print("Archivos limpios guardados en ", OUTPUT_DIR)


if __name__ == "__main__":
    limpiar_datos()