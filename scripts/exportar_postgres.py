#imports necesarios

import polars as pl
import json
import os
from pathlib import Path

#Ruta de entrada
INPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"


#Armar conexión
def obtener_conn_uri():
    password = os.environ.get("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Exportar variable POSTGRES_PASSWORD para el script")
    #URI de conexion en formato ADBC (sin +psycopg2)
    return f"postgresql://favorita_user:{password}@localhost:5432/favorita_db"


def exportar_postgres():
    conn_uri = obtener_conn_uri()

    df = pl.read_parquet(INPUT_DIR / "consolidado.parquet")
    print("Exportando consolidado a Postgres...", df.shape)
    df.write_database(
        table_name="consolidado",
        connection=conn_uri,
        if_table_exists="replace",
        engine="adbc",
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
                connection=conn_uri,
                if_table_exists="replace",
                engine="adbc",
            )
            print(f"Tabla '{nombre}' exportada ({tabla.shape[0]} filas).")
        else:
            print(f"'{nombre}' no es una lista de resultados, se omite.")


if __name__ == "__main__":
    exportar_postgres()
