import polars as pl
import json
from pathlib import Path
from consolidar import consolidar

#Importar timedelta para calcular la diferencia de tiempo entre fechas
from datetime import timedelta

INPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"
OUTPUT_DIR = Path.home() / "favorita_pipeline" / "data" / "processed"


def eda_profundo():
    #cargar el parquet actualizado de consolidado
    df = pl.read_parquet(INPUT_DIR / "consolidado.parquet")
    print("Consolidado cargado: ", df.shape)
    print("Columnas disponibles:", df.columns)

    resultados = {}

    #VENTAS POR FAMILIA DE PRODUCTO----------------------------------------------------------------------------------
    ventas_por_familia = (
        df.group_by("family").agg(pl.col("sales").sum().alias("ventas_totales")).sort("ventas_totales", descending=True)
    )
    print("Ventas por familia de producto: ")
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
    resultados["ventas_promedio_por_provincia"] = ventas_por_provincia.to_dicts()

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

    #ESTACIONALIDAD Y FERIADOS----------------------------------------------------------------------------------
    #Impacto de feriados nacionales: comparacion días feriados vs días normales
    df = df.with_columns(
        (
            (pl.col("locale") == "National")
            & (pl.col("type_right") != "Work Day")
            & (pl.col("transferred") != True)
        ).fill_null(False).alias("es_feriado_nacional")
    )
    ventas_feriado_vs_normal = (
        df.group_by("es_feriado_nacional")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )
    print("Ventas promedio: feriados nacionales vs días normales: ")
    print(ventas_feriado_vs_normal)
    resultados["ventas_feriado_vs_normal"] = ventas_feriado_vs_normal.to_dicts()

    #VENTANA DE 3 DIAS ANTES/DESPUES DE FERIADOS, POR FAMILIA
    fechas_feriado = (
        df.filter(pl.col("es_feriado_nacional"))
        .select("date")
        .unique()
        .to_series()
        .to_list()
    )
    filas_ventana = []
    for fecha_feriado in fechas_feriado:
        for offset in [-3, -2, -1, 1, 2, 3]:
            filas_ventana.append({
                "date": fecha_feriado + timedelta(days=offset),
                "dias_relativo_feriado": offset,
            })
    ventana_df = pl.DataFrame(filas_ventana)
    df_ventana = df.join(ventana_df, on="date", how="inner")
    ventas_ventana_feriados = (
        df_ventana.group_by(["family", "dias_relativo_feriado"])
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort(["family", "dias_relativo_feriado"])
    )
    print("Ventas promedio en ventana +-3 dias de feriados, por familia:")
    print(ventas_ventana_feriados)
    resultados["ventas_ventana_feriados_por_familia"] = ventas_ventana_feriados.to_dicts()

    #FAMILIAS MAS SENSIBLES A FERIADOS
    ventas_familia_feriado = (
        df.group_by(["family", "es_feriado_nacional"])
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )
    pivot = ventas_familia_feriado.pivot(
        on="es_feriado_nacional",
        index="family",
        values="venta_promedio"
    )
    pivot = pivot.rename({"true": "venta_feriado", "false": "venta_normal"})
    pivot = pivot.with_columns(
        (
            (pl.col("venta_feriado") - pl.col("venta_normal")) / pl.col("venta_normal") * 100
        ).alias("cambio_porcentual")
    )
    sensibilidad_familias = pivot.sort("cambio_porcentual", descending=True)
    print("Sensibilidad de familias a feriados: ")
    print(sensibilidad_familias)
    resultados["sensibilidad_familias_feriados"] = sensibilidad_familias.to_dicts()

    #PROMOCIONES----------------------------------------------------------------------------------
    #Comparacion de ventas con y sin promocion, por familia
    ventas_promocion_familia = (
        df.group_by(["family", "onpromotion"])
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )
    pivot_promo = ventas_promocion_familia.pivot(
        on="onpromotion",
        index="family",
        values="venta_promedio"
    )
    #Las columnas quedan nombradas segun los valores unicos de onpromotion (0 y 1 o rangos numericos)
    print("Comparacion ventas con y sin promocion por familia (pivot crudo): ")
    print(pivot_promo)
    resultados["ventas_promocion_vs_sin_promocion_familia"] = pivot_promo.to_dicts()

    #Efecto general de promociones: ventas promedio con promocion (onpromotion > 0) vs sin promocion (onpromotion == 0)
    df = df.with_columns((pl.col("onpromotion") > 0).alias("tiene_promocion"))
    efecto_promocion_familia = (
        df.group_by(["family", "tiene_promocion"])
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )
    pivot_efecto = efecto_promocion_familia.pivot(
        on="tiene_promocion",
        index="family",
        values="venta_promedio"
    )
    pivot_efecto = pivot_efecto.rename({"true": "venta_con_promocion", "false": "venta_sin_promocion"})
    pivot_efecto = pivot_efecto.with_columns(
        (
            (pl.col("venta_con_promocion") - pl.col("venta_sin_promocion")) / pl.col("venta_sin_promocion") * 100
        ).alias("incremento_porcentual")
    )
    efecto_promociones_ordenado = pivot_efecto.sort("incremento_porcentual", descending=True)
    print("Efecto de promociones por familia (ordenado por mayor incremento): ")
    print(efecto_promociones_ordenado)
    resultados["efecto_promociones_por_familia"] = efecto_promociones_ordenado.to_dicts()
    #R: en el archivo resultados_eda_profundo se guarda el detalle; las familias con mayor incremento_porcentual son las mas beneficiadas por promociones

    #PETROLEO Y ECONOMIA----------------------------------------------------------------------------------
    #Correlacion entre precio del petroleo y ventas totales mensuales
    ventas_petroleo_mes = (
        df.with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["anio", "mes"])
        .agg([
            pl.col("sales").sum().alias("ventas_totales_mes"),
            pl.col("dcoilwtico").mean().alias("petroleo_promedio_mes"),
        ])
        .sort(["anio", "mes"])
    )
    correlacion_petroleo_ventas = ventas_petroleo_mes.select(
        pl.corr("ventas_totales_mes", "petroleo_promedio_mes").alias("correlacion")
    ).item()
    print("Correlacion precio petroleo vs ventas totales mensuales: ", correlacion_petroleo_ventas)
    resultados["correlacion_petroleo_ventas_mensual"] = correlacion_petroleo_ventas
    resultados["ventas_petroleo_por_mes"] = ventas_petroleo_mes.to_dicts()

    #Lag temporal entre caida del petroleo y caida en ventas (periodo 2015-2016)
    ventas_petroleo_periodo = ventas_petroleo_mes.filter(
        (pl.col("anio") >= 2015) & (pl.col("anio") <= 2016)
    ).sort(["anio", "mes"])

    correlaciones_lag = []
    for lag in range(0, 7):
        ventas_petroleo_lag = ventas_petroleo_periodo.with_columns(
            pl.col("petroleo_promedio_mes").shift(lag).alias("petroleo_lag")
        )
        corr_lag = ventas_petroleo_lag.select(
            pl.corr("ventas_totales_mes", "petroleo_lag").alias("correlacion")
        ).item()
        correlaciones_lag.append({"lag_meses": lag, "correlacion": corr_lag})

    print("Correlacion por lag temporal (2015-2016): ")
    print(correlaciones_lag)
    resultados["correlacion_lag_petroleo_ventas_2015_2016"] = correlaciones_lag
    #R: el lag con mayor correlacion (en valor absoluto) indica cuantos meses tarda una caida del petroleo en reflejarse en las ventas

    #Ciudades con mayor sensibilidad a la caida del petroleo
    ventas_petroleo_ciudad_mes = (
        df.with_columns([
            pl.col("date").dt.year().alias("anio"),
            pl.col("date").dt.month().alias("mes"),
        ])
        .group_by(["city", "anio", "mes"])
        .agg([
            pl.col("sales").sum().alias("ventas_totales_mes"),
            pl.col("dcoilwtico").mean().alias("petroleo_promedio_mes"),
        ])
    )
    correlacion_por_ciudad = (
        ventas_petroleo_ciudad_mes.group_by("city")
        .agg(pl.corr("ventas_totales_mes", "petroleo_promedio_mes").alias("correlacion"))
        .sort("correlacion")
    )
    print("Correlacion precio petroleo vs ventas, por ciudad (mas sensibles primero): ")
    print(correlacion_por_ciudad)
    resultados["correlacion_petroleo_por_ciudad"] = correlacion_por_ciudad.to_dicts()

    #TRANSACCIONES----------------------------------------------------------------------------------
    #Relacion entre numero de transacciones y volumen de ventas por tienda
    transacciones_unicas = (
    df.select(["store_nbr", "date", "transactions"])
    .unique()
    )
    transacciones_por_tienda = (
    transacciones_unicas.group_by("store_nbr")
    .agg(pl.col("transactions").sum().alias("transacciones_totales"))
    )
    ventas_por_tienda = (
    df.group_by("store_nbr")
    .agg(pl.col("sales").sum().alias("ventas_totales"))
    )
    transacciones_ventas_tienda = ventas_por_tienda.join(transacciones_por_tienda, on="store_nbr")
    transacciones_ventas_tienda = transacciones_ventas_tienda.with_columns(
        (pl.col("ventas_totales") / pl.col("transacciones_totales")).alias("ticket_promedio")
    )
    print("Relacion ventas y transacciones por tienda: ")
    print(transacciones_ventas_tienda.sort("ticket_promedio", descending=True))
    resultados["relacion_transacciones_ventas_por_tienda"] = transacciones_ventas_tienda.to_dicts()

    #Tiendas con ticket promedio alto vs ticket bajo
    ticket_alto = transacciones_ventas_tienda.sort("ticket_promedio", descending=True).head(10)
    ticket_bajo = transacciones_ventas_tienda.sort("ticket_promedio", descending=False).head(10)
    print("Top 10 tiendas con ticket promedio mas alto: ")
    print(ticket_alto)
    print("Top 10 tiendas con ticket promedio mas bajo: ")
    print(ticket_bajo)
    resultados["top_10_ticket_alto"] = ticket_alto.to_dicts()
    resultados["top_10_ticket_bajo"] = ticket_bajo.to_dicts()

    #Guardar todos los resultados en un archivo JSON para consulta posterior y para exportar_postgres
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "resultados_eda_profundo.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2, default=str)
    print()
    print("Resultados guardados en", OUTPUT_DIR / "resultados_eda_profundo.json")

    return resultados


if __name__ == "__main__":
    eda_profundo()
