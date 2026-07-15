#imports necesarios

import polars as pl
import json
import os
from pathlib import Path
from sqlalchemy import create_engine

#Ruta de entrada
INPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"


#Armar conexión
def obtener_engine():
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Exportar variable POSTGRES_PASSWORD para el script")
    
    return create_engine(
        #URL de conexion a la base de datos PostgreSQL
        f"postgresql+psycopg2://favorita_user:{password}@localhost:5432/favorita"
    )


def exportar_postgres():
    engine = obtener_engine()

    df = pl.read_parquet(INPUT_DIR / "consolidado.parquet")
    print("Exportando consolidado a Postgres...", df.shape)

    df.write_database(
        table_name="consolidado",
        connection=engine,
        if_table_exists="replace",
    )
    print("Consolidado exportado correctamente")

    #Exportar resultados del eda_profundo en tablas separadas
    with open(INPUT_DIR / "resultados_eda_profundo.json", "r", encoding="utf-8") as f:
        resultados = json.load(f)

    for nombre, valor in resultados.items():
        if isinstance(valor, list) and len(valor) > 0 and isinstance(valor[0], dict):
            tabla = pl.DataFrame(valor)
            tabla.write_database(
                table_name=nombre,
                connection=engine,
                if_table_exists="replace",
            )
            print(f"Tabla '{nombre}' exportada ({tabla.shape[0]} filas).")
        else:
            print(f"'{nombre}' no es una lista de resultados, se omite.")


if __name__ == "__main__":
    exportar_postgres()
