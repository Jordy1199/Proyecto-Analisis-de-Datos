# Proyecto Favorita - Pipeline de Análisis de Datos

**Integrantes:**Jordy Cajas, Briander Verdezoto

## 1. Descripción del proyecto

Pipeline de datos automatizado para el procesamiento del dataset "Store Sales - Time Series Forecasting" de la Corporación Favorita. El desarrollo de tareas se encuentra orquestado con Apache Airflow, haciendo la ejecución de 6 tareas secuancialmente: carga de datos, diagnóstico de calidad (EDA inicial), limpieza, consolidación de las 5 tablas fuente, análisis exploratorio profundo (EDA profundo) y exportación de resultados a PostgreSQL. Los resultados finales serán visualizados mediante un dashboard de Power BI conectado en modo DirectQuery.

**Infraestructura:** Máquina Virtual en Azure (Ubuntu Server), PostgreSQL, Apache Airflow, Power BI Desktop.

## 2. Descripción de los archivos del dataset y su rol en el pipeline

| Archivo | Contenido | Rol en el pipeline |
|---|---|---|
| `train.csv` | Ventas diarias por tienda y familia de producto (2013-2017), con indicador de promoción. | Tabla central. Se consolida con las demás mediante joins por `store_nbr` y `date`. Base de todo el EDA. |
| `stores.csv` | Metadata de cada tienda: ciudad, provincia (`state`), tipo y cluster. | Se une a `train` por `store_nbr` para poder analizar ventas por ubicación geográfica. |
| `oil.csv` | Precio diario del petróleo WTI. | Se une por `date`. Único archivo con valores nulos reales (días sin cotización de mercado); se limpia con interpolación lineal. |
| `holidays_events.csv` | Calendario de feriados, eventos y días de traslado, con alcance (nacional/regional/local). | Se une por `date`. Permite identificar feriados nacionales reales y analizar su impacto en ventas. |
| `transactions.csv` | Número de transacciones por tienda y día. | Se une por `store_nbr` y `date`. Permite calcular el ticket promedio (ventas / transacciones) por tienda. |

Los 5 archivos se descargan de Kaggle o del enlace onedrive proporcionado y **no se versionan en este repositorio**; es necesario que cada integrante los descarga localmente en `data/raw/` para ejecutar el pipeline correctamente.

## 3. Diagrama de arquitectura de la solución

<img width="407" height="346" alt="DiagramaSolucion" src="https://github.com/user-attachments/assets/f364020b-59de-412f-87b3-feecf4a87d11" />

Los 5 CSV se descargan de Kaggle y se cargan en una VM de Azure (Ubuntu Server).
Dentro de la VM, Apache Airflow orquesta las 6 tareas del pipeline (carga, EDA
inicial, limpieza, consolidación, EDA profundo y exportación), escribiendo los
resultados finales en una base de datos PostgreSQL que corre en la misma VM.
Power BI Desktop se conecta a PostgreSQL en modo DirectQuery para mostrar el
dashboard con datos siempre actualizados.

## Estructura
- `dags/`: definición del DAG de Airflow
- `scripts/`: scripts de cada tarea del pipeline
- `manifest.json`: metadatos del pipeline

## Requisitos
Python 3.10+, Apache Airflow 3.1.0, Polars, PostgreSQL 16
