import polars as pl
import json
from pathlib import Path
from consolidar import consolidar

INPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"
OUTPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"



def eda_profundo():
    #cargar el parquet actualizado de consolidado
    df = pl.read_parquet(INPUT_DIR / "consolidado.parquet")
    print("Consolidado cargado: ", df.shape)

    resultados = {}

    #VENTAS POR FAMILIA DE PRODUCTO----------------------------------------------------------------------------------
    ventas_por_familia = (
        df.group_by("family").agg(pl.col("sales").sum().alias("ventas_totales")).sort("ventas_totales", descending=True)
    )

    print ("Ventas por familia de producto: ")
    print(ventas_por_familia)
    resultados["ventas_por_familia"] = ventas_por_familia.to_dicts()
    #R: Las categorias que presentan mayor volumen son Grocery I, Beverages, Produce, Cleaning y Dairy con ventas sobrepasando los 65 millones hasta 350 millones

    #Ventas totales por tienda y ranking
    ventas_totales_por_tienda = (
        df.group_by("store_nbr").agg(pl.col("sales").sum().alias("ventas_totales")).sort("ventas_totales", descending=True)
    )

    top_10_mayores = ventas_totales_por_tienda.head(10)
    top_10_menores = ventas_totales_por_tienda.tail(10)

    print("Top 10 tiendas con mayores ventas: ")
    print(top_10_mayores)
    print("Top 10 tiendas con menores ventas: ")
    print(top_10_menores)

    resultados["top_10_tiendas_mayor_venta"] = top_10_mayores.to_dicts()
    resultados["top_10_tiendas_menor_venta"] = top_10_menores.to_dicts()
    
    #R: en el archivo resultados_eda_profundo

    #Ventas promedio por ciudad y provincia
    ventas_por_ciudad = (
        df.group_by("city")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort("venta_promedio", descending=True)
    )

    ventas_por_provincia = (
        df.group_by("state")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort("venta_promedio", descending=True)
    )

    print("Ventas promedio por ciudad: ")
    print(ventas_por_ciudad)
    print("Ventas promedio por provincia: ")
    print(ventas_por_provincia)

    resultados["ventas_promedio_por_ciudad"] = ventas_por_ciudad.to_dicts()
    resultados["ventas_promedio_por_provincia"] = ventas_por_provincia.to_dict()

    #R: En el archivo resultados_eda_profundo.json


    #Evolución temporal de ventas: tendencia mensual y anual entre 2013 y 2017

    ventas_por_anio = (
        df.with_columns(pl.col("date").dt.year().alias("anio"))
        .group_by("anio").agg(pl.col("sales").sum().alias("ventas_totales")).sort("anio")
    )

    ventas_por_mes = (
        df.with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["anio", "mes"]).agg(pl.col("sales").sum().alias("ventas_totales")).sort(["anio", "mes"])
    )
    print("Ventas por año: ")
    print(ventas_por_anio)
    print("Ventas por mes: ")
    print(ventas_por_mes)

    resultados["ventas_por_anio"] = ventas_por_anio.to_dicts()
    resultados["ventas_por_mes"] = ventas_por_mes.to_dicts()


    return resultados


if __name__ == "__main__":
    eda_profundo()