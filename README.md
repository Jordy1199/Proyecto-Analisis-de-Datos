# Proyecto Favorita - Pipeline de Análisis de Datos

**Integrantes:** Jordy Cajas, Briander Verdezoto

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

![Arquitectura del pipeline](C:\Users\pochi\Downloads\WEAS X\Weas del BRIANDER-N DELUXE\UNIVERSIDAD\Análisis\PROYECTO\DiagramaSolucion.png)

Los 5 CSV se descargan de Kaggle y se cargan en una VM de Azure (Ubuntu Server).
Dentro de la VM, Apache Airflow orquesta las 6 tareas del pipeline (carga, EDA
inicial, limpieza, consolidación, EDA profundo y exportación), escribiendo los
resultados finales en una base de datos PostgreSQL que corre en la misma VM.
Power BI Desktop se conecta a PostgreSQL en modo DirectQuery para mostrar el
dashboard con datos siempre actualizados.

## 4. Descripción del DAG: tareas, dependencias y configuración

**DAG ID:** `favorita_pipeline`
**Estilo:** TaskFlow API (`@dag` / `@task`)

### Configuración del DAG

| Parámetro | Valor | Motivo |
|---|---|---|
| `schedule` | `None` | Ejecución manual, disparada por el equipo o por integración con GitHub Actions. |
| `start_date` | `2026-01-01` | Fecha de referencia para el cálculo de ejecuciones. |
| `catchup` | `False` | Evita que Airflow intente ejecutar corridas "atrasadas" desde el `start_date`. |
| `tags` | `favorita`, `analisis_datos` | Facilitan filtrar el DAG en la interfaz de Airflow. |

### Tareas y su función

| Tarea | Script que ejecuta | Función |
|---|---|---|
| `cargar_datos_task` | `cargar_datos.py` | Lee los 5 CSV originales con Polars. |
| `eda_inicial_task` | `eda_inicial.py` | Diagnostica nulos, duplicados y tipos de cada tabla; genera `eda_inicial.json`. |
| `limpiar_datos_task` | `limpiar_datos.py` | Elimina duplicados e imputa nulos (interpolación en el precio del petróleo). |
| `consolidar_task` | `consolidar.py` | Une las 5 tablas limpias en un solo DataFrame (`consolidado.parquet`). |
| `eda_profundo_task` | `eda_profundo.py` | Responde las preguntas de negocio de la sección 6.2; genera `resultados_eda_profundo.json`. |
| `exportar_postgres_task` | `exportar_postgres.py` | Sube el consolidado y cada resultado del EDA a PostgreSQL. |

### Dependencias (orden de ejecución)

Cada tarea recibe el resultado (un mensaje de confirmación, no los datos en sí) de la
tarea anterior como parámetro, lo que le indica a Airflow el orden de ejecución. Los
datos reales se pasan entre tareas a través de archivos `.parquet` intermedios en
`data/processed/`, no en memoria — esto respeta el modelo de ejecución aislada de
Airflow (cada tarea corre como un proceso independiente).

\`\`\`
cargar_datos_task → eda_inicial_task → limpiar_datos_task → consolidar_task → eda_profundo_task → exportar_postgres_task
\`\`\`

## 5. Proceso del pipeline: descripción de cada etapa

### Vista general del DAG

<img width="1166" height="277" alt="5 1" src="https://github.com/user-attachments/assets/721b5f94-99a4-4d64-b3c6-c5528575c7ad" />


Las 6 tareas se ejecutan en orden secuencial. Cada caja verde indica ejecución
exitosa; en caso de fallo, Airflow detiene la cadena y marca las tareas
posteriores como `upstream_failed`, sin ejecutarlas.

### Etapa 1 — Carga de datos (`cargar_datos_task`)

Lee los 5 archivos CSV originales con Polars, usando `try_parse_dates=True` para
detectar automáticamente las columnas de fecha. Imprime la forma (filas, columnas)
de cada tabla como confirmación.

<img width="1523" height="445" alt="5 2" src="https://github.com/user-attachments/assets/bea662da-9cf1-4e3d-9f6a-ae31f5949a3b" />


### Etapa 2 — EDA inicial (`eda_inicial_task`)

Calcula, por cada tabla: número de filas/columnas, tipos de dato, nulos por
columna y duplicados. El único hallazgo relevante: 43 nulos en la columna
`dcoilwtico` de `oil.csv` (días sin cotización de mercado); el resto de tablas
no presentó nulos ni duplicados.

<img width="1483" height="512" alt="5 3" src="https://github.com/user-attachments/assets/49660104-8b1e-4926-b800-2c21de7fb86d" />


### Etapa 3 — Limpieza (`limpiar_datos_task`)

Elimina duplicados exactos con `.unique()`. Imputa los nulos de `oil` mediante
interpolación lineal (`.interpolate()`), con relleno hacia adelante/atrás como
respaldo para los extremos de la serie.

<img width="1512" height="529" alt="5 4" src="https://github.com/user-attachments/assets/f529c351-3cd9-49e5-9302-514524ab9e96" />


### Etapa 4 — Consolidación (`consolidar_task`)

Une las 5 tablas limpias mediante `.join()`: `train` + `stores` (por `store_nbr`),
+ `transactions` (por `store_nbr` y `date`), + `oil` (por `date`), + `holidays`
(por `date`). Resultado: una tabla consolidada de 3,054,348 filas y 17 columnas.

<img width="1515" height="591" alt="5 5" src="https://github.com/user-attachments/assets/7ef4a725-f370-4038-882e-2f9f689f4ed9" />


### Etapa 5 — EDA profundo (`eda_profundo_task`)

Responde las preguntas de negocio de las 5 secciones del enunciado (ventas
generales, estacionalidad/feriados, promociones, petróleo, transacciones).
Guarda cada resultado en `resultados_eda_profundo.json`.

<img width="1526" height="499" alt="5 6" src="https://github.com/user-attachments/assets/a5313a45-3b8e-497a-8c5c-95c4b56463dc" />


### Etapa 6 — Exportación a PostgreSQL (`exportar_postgres_task`)

Sube la tabla consolidada completa y 18 tablas adicionales (una por cada
resultado del EDA profundo) a PostgreSQL, listas para ser consumidas por
Power BI en modo DirectQuery.

<img width="1541" height="526" alt="5 7" src="https://github.com/user-attachments/assets/6aa82f42-213f-40bf-861d-7b9416ad4a54" />

### Tiempos de ejecución

<img width="1094" height="551" alt="5 8" src="https://github.com/user-attachments/assets/964d00e8-e4fb-4398-af8d-a751174b0335" />

## 6. Métricas del pipeline

### Tiempo de ejecución por tarea

| Tarea | Duración |
|---|---|
| `cargar_datos_task` | 0.58 s |
| `eda_inicial_task` | 3.39 s |
| `limpiar_datos_task` | 4.21 s |
| `consolidar_datos_task` | 5.62 s |
| `eda_profundo_task` | 7.84 s |
| `exportar_postgres_task` | ~10.34 s |
| **Total del DAG** | **31.98 s** |

### Registros procesados por etapa

| Etapa | Filas |
|---|---|
| `train.csv` (entrada) | 3,000,888 |
| `stores.csv` | 54 |
| `oil.csv` | 1,218 |
| `holidays_events.csv` | 350 |
| `transactions.csv` | 83,488 |
| Consolidado final | 3,054,348 |

El consolidado tiene más filas que `train` original porque algunas fechas
registran más de un feriado simultáneo (nacional + regional/local); al unir por
`date`, esas filas se multiplican una vez por cada coincidencia — comportamiento
esperado de un `join`, no una duplicación indebida.

### Registros eliminados/corregidos en limpieza

| Verificación | Resultado |
|---|---|
| Duplicados detectados (`.is_duplicated()`) en las 5 tablas | 0 |
| Nulos detectados | 43 (columna `dcoilwtico` de `oil.csv`) |
| Filas eliminadas | 0 |
| Valores imputados (interpolación lineal + relleno de extremos) | 43 |

El dataset original ya venía curado por los organizadores de la competencia
(Kaggle); el único vacío real correspondía a días sin cotización de mercado
para el precio del petróleo (fines de semana y feriados bursátiles).

## 9. Conclusiones y recomendaciones

### Conclusiones

- **Concentración de ventas por categoría:** Grocery I domina las ventas totales
  (~350M), 58% por encima de Beverages, la segunda categoría. Cinco familias
  (Grocery I, Beverages, Produce, Cleaning, Dairy) concentran la mayoría del
  volumen de ventas de toda la cadena.

- **Los feriados nacionales impactan positivamente las ventas promedio**
  ($425 en feriado vs $353 en día normal), pero el efecto no es uniforme entre
  familias de producto — algunas categorías son mucho más sensibles que otras.

- **Existe una relación clara entre transacciones y ventas por tienda**
  (correlación visualmente fuerte en el gráfico de dispersión), lo que confirma
  que el volumen de clientes es el principal impulsor de ingresos, más que el
  ticket promedio individual.

- **La correlación entre precio del petróleo y ventas mensuales es negativa**,
  y se vuelve más marcada con un desfase (lag) de varios meses durante el
  período 2015-2016, sugiriendo que el impacto económico de la caída del
  petróleo en Ecuador no fue inmediato sobre el consumo.

- **La calidad del dataset original era alta**: 0 duplicados y prácticamente
  0 nulos fuera de la serie de precios del petróleo, lo que permitió enfocar
  el esfuerzo del pipeline en la consolidación y el análisis, más que en
  limpieza extensiva.

### Recomendaciones

- **Pipeline incremental:** actualmente el pipeline reprocesa el dataset
  completo en cada corrida (`if_table_exists="replace"`). Para un entorno de
  producción real, se recomendaría procesar solo los datos nuevos, reduciendo
  tiempo de ejecución a medida que el dataset crezca.

- **Monitoreo y alertas:** agregar notificaciones (correo/Slack) en caso de
  fallo de alguna tarea, en vez de depender de revisar manualmente la interfaz
  de Airflow.

- **Modelo predictivo como siguiente fase:** el EDA profundo identificó
  patrones claros (estacionalidad, sensibilidad a promociones, relación con
  el petróleo) que podrían alimentar un modelo de forecasting de ventas —
  explícitamente fuera del alcance de este proyecto, pero un paso natural
  siguiente.

- **Ampliar el análisis geográfico:** dado que se detectaron diferencias de
  sensibilidad al petróleo por ciudad, valdría la pena profundizar con datos
  socioeconómicos adicionales por región para entender el porqué de esas
  diferencias.
