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


## 7. Capturas del dashboard de Power BI

### Página 1 — Ventas Generales

- **Suma de ventas_totales por family:** ranking de familias de producto por volumen de venta. Grocery I lidera con amplio margen (~0,4 mil M), seguido de Beverages y Produce.
<img width="1033" height="464" alt="image" src="https://github.com/user-attachments/assets/938c9855-070a-4b94-9b55-ceef48dabc50" />

- **Suma de ventas_totales por mes:** evolución mensual agregada de ventas a lo largo del año, mostrando un pico entre los meses 6-7 y una caída marcada en el mes 9.
<img width="943" height="463" alt="image" src="https://github.com/user-attachments/assets/9c1aa91f-3285-4ecd-bfc6-9e77de06a77e" />

- **Ventas promedio por provincia:** mapa geográfico de Ecuador con burbujas proporcionales a la venta promedio por provincia; se concentra visualmente en Guayaquil y Quito.
<img width="1054" height="468" alt="image" src="https://github.com/user-attachments/assets/1d91541c-ea44-47e1-8072-241d4a18d8ca" />

- **Impacto de feriados en ventas promedio:** comparación directa entre ventas en feriado nacional ($425,43) vs día normal ($353,34), confirmando el incremento identificado en el EDA profundo.
<img width="944" height="445" alt="image" src="https://github.com/user-attachments/assets/8c6ebb64-37a5-4866-9853-1e5c3c12366e" />

### Página 2 — Promociones y Economía

- **Ventas mensuales vs precio del petróleo:** gráfico combinado (barras + línea) que superpone ventas totales mensuales con el precio promedio del petróleo, permitiendo observar visualmente la relación inversa entre ambas series a lo largo de 2015-2018.
<img width="1154" height="451" alt="image" src="https://github.com/user-attachments/assets/438b5361-e0e3-45d8-b743-04ac0246537c" />


- **Top 10 tiendas - Mayor venta:** ranking de las 10 tiendas (por `store_nbr`) con mayor venta acumulada.
<img width="816" height="455" alt="image" src="https://github.com/user-attachments/assets/36ce9e70-48c6-4dee-9808-1b4fb84fe085" />

- **Ventas con promoción vs sin promoción por familia:** comparación de barras dobles por familia de producto, mostrando el efecto de las promociones en cada categoría — Grocery I y Beverages muestran la mayor diferencia absoluta.
<img width="1150" height="448" alt="image" src="https://github.com/user-attachments/assets/ebc55d44-85a7-4c90-8304-97ca6c6a5292" />

- **Top 10 tiendas - Menor venta:** ranking de las 10 tiendas con menor venta acumulada, útil para identificar puntos de venta con bajo desempeño.
<img width="819" height="449" alt="image" src="https://github.com/user-attachments/assets/e3d06052-a9a8-4aa1-9449-7f02e574171b" />

### Página 3 — Transacciones y Sensibilidad

- **Relación entre transacciones y ventas totales por tienda:** gráfico de dispersión que confirma una correlación fuerte y positiva — a mayor número de transacciones, mayor venta total, con una tendencia lineal clara.
<img width="1053" height="494" alt="image" src="https://github.com/user-attachments/assets/cddc629c-1041-4e6e-8e31-ffa2277afbc9" />

- **Top 10 tiendas - Ticket promedio bajo:** tiendas con menor venta promedio por transacción (alto volumen de clientes, compras pequeñas).
<img width="620" height="389" alt="image" src="https://github.com/user-attachments/assets/53b7fd56-fa1f-47bb-a53e-ca83660909cb" />

- **Correlación petróleo-ventas según lag temporal (2015-2016):** barras que muestran cómo cambia la correlación entre el precio del petróleo y las ventas al desplazar la comparación entre 0 y 6 meses, identificando en qué punto el efecto económico se refleja con mayor fuerza en el consumo.
<img width="1058" height="256" alt="image" src="https://github.com/user-attachments/assets/f6aff026-c331-48b4-a655-11fdaec432ab" />

- **Top 10 tiendas - Ticket promedio alto:** tiendas con mayor venta promedio por transacción (menor volumen de clientes, compras de mayor valor).
<img width="661" height="397" alt="image" src="https://github.com/user-attachments/assets/4e9909c7-6250-4cba-ae91-96c06b92673a" />

## 8. Despliegue: instrucciones para reproducir el ambiente

### Requisitos previos

- Cuenta de Azure for Students con crédito activo.
- Cliente SSH (WSL2, terminal Linux o similar).
- Git instalado localmente.

### Paso 1 — Crear la máquina virtual en Azure

| Parámetro | Valor |
|---|---|
| Suscripción | Azure for Students |
| Grupo de recursos | `rg-favorita-pipeline` |
| Región | Mexico Central |
| Imagen | Ubuntu Server 24.04 LTS (Gen2) |
| Tamaño | Standard_B2s (2 vCPUs, 4 GB RAM) |
| Autenticación | Clave pública SSH |
| Puertos de entrada | SSH (22) |

En el Network Security Group de la VM, agregar además una regla de entrada para el puerto **8080/TCP** (interfaz web de Airflow).

### Paso 2 — Conectarse a la VM

```bash
ssh -i <clave_privada>.pem <usuario>@<IP_publica_VM>
```

### Paso 3 — Preparar el sistema operativo

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv postgresql postgresql-contrib git tmux -y
```

### Paso 4 — Clonar el repositorio

```bash
git clone https://github.com/Jordy1199/Proyecto-Analisis-de-Datos.git
cd Proyecto-Analisis-de-Datos
```

### Paso 5 — Crear entorno virtual e instalar dependencias

```bash
python3 -m venv venv
source venv/bin/activate
pip install "apache-airflow==3.1.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.1.0/constraints-3.12.txt"
pip install polars psycopg2-binary sqlalchemy "pandas==2.1.4" adbc-driver-postgresql adbc-driver-manager
```

El uso del archivo `--constraint` oficial de Apache Airflow es obligatorio: evita conflictos de versión entre las dependencias internas de Airflow (`structlog`, `starlette`, `pyjwt`, entre otras) que de otro modo `pip` instalaría en sus versiones más recientes e incompatibles entre sí.

### Paso 6 — Configurar PostgreSQL

```bash
sudo -u postgres psql -c "CREATE DATABASE favorita_db;"
sudo -u postgres psql -c "CREATE USER favorita_user WITH PASSWORD '<definir_contraseña>';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE favorita_db TO favorita_user;"
```

### Paso 7 — Configurar variable de entorno para la contraseña

El script `exportar_postgres.py` lee la contraseña desde una variable de entorno, nunca desde el código fuente:

```bash
echo 'export POSTGRES_PASSWORD="<la_misma_contraseña_del_paso_6>"' >> ~/.bashrc
source ~/.bashrc
```

### Paso 8 — Copiar los datos crudos a la VM

Los 5 archivos CSV **no están en el repositorio**. Deben copiarse manualmente a:
~/favorita_pipeline/data/raw/store-sales-time-series-forecasting/

Ejemplo, desde la máquina local:

```bash
scp -i <clave_privada>.pem <ruta_local_csvs>/*.csv <usuario>@<IP_VM>:~/favorita_pipeline/data/raw/store-sales-time-series-forecasting/
```

### Paso 9 — Enlazar la carpeta de scripts

```bash
ln -s ~/Proyecto-Analisis-de-Datos/scripts ~/favorita_pipeline/scripts
```

### Paso 10 — Inicializar y levantar Airflow

```bash
export AIRFLOW_HOME=~/Proyecto-Analisis-de-Datos
airflow db migrate

tmux new -s airflow
export AIRFLOW_HOME=~/Proyecto-Analisis-de-Datos
source venv/bin/activate
airflow standalone
```

Salir de la sesión de tmux sin detener el proceso: `Ctrl+B`, luego `D`.

### Paso 11 — Acceder a la interfaz web

- URL: `http://<IP_publica_VM>:8080`
- Usuario: `admin`
- Contraseña: generada automáticamente en:
```bash
cat ~/Proyecto-Analisis-de-Datos/simple_auth_manager_passwords.json.generated
```

### Paso 12 — Ejecutar el pipeline

Desde la interfaz web (botón "Trigger"), por terminal:
```bash
airflow dags trigger favorita_pipeline
```
o automáticamente mediante GitHub Actions (ver sección de CI/CD), cada vez que se actualiza `manifest.json`.

### Administración de la VM (encendido/apagado)

```bash
# Apagar (libera cómputo, evita cargos innecesarios)
az vm deallocate --resource-group rg-favorita-pipeline --name vm-favorita

# Encender
az vm start --resource-group rg-favorita-pipeline --name vm-favorita
```

### Paso 13 — Conectar Power BI a PostgreSQL

Power BI Desktop se conecta directamente a la base `favorita_db` en modo DirectQuery, sin necesidad de exportar archivos intermedios:

1. En Power BI Desktop: **Obtener datos → Base de datos → PostgreSQL**.
2. Servidor: `<IP_publica_VM>:5432`
3. Base de datos: `favorita_db`
4. Modo de conectividad de datos: **DirectQuery** (mantiene el dashboard sincronizado con cada nueva ejecución del pipeline, sin recargar manualmente).
5. Credenciales: usuario `favorita_user` y su contraseña.
6. Seleccionar las tablas necesarias (`consolidado` y las tablas generadas por el EDA profundo).

**Requisito de red:** el puerto **5432/TCP** debe estar abierto en el Network Security Group de la VM para permitir la conexión remota desde Power BI.

### CI/CD — Automatización con GitHub Actions

El repositorio incluye un workflow (`.github/workflows/trigger_pipeline.yml`) que dispara automáticamente la ejecución del DAG en la VM, sin necesidad de conectarse manualmente por SSH.

**Disparadores:**
- Automático: cualquier `push` que modifique `manifest.json` en la raíz del repositorio.
- Manual: mediante `workflow_dispatch`, desde la pestaña "Actions" de GitHub.

**Funcionamiento:**
1. Decodifica una clave SSH almacenada como secreto de GitHub (`VM_SSH_KEY`, en base64).
2. Se conecta por SSH a la VM (usando los secretos `VM_HOST` y `VM_USER`) mediante la acción `appleboy/ssh-action`.
3. Dentro de la VM, activa el entorno virtual y ejecuta `airflow dags trigger favorita_pipeline`.

**Secretos configurados en el repositorio** (Settings → Secrets and variables → Actions):
- `VM_SSH_KEY`: clave privada SSH codificada en base64.
- `VM_HOST`: IP pública de la VM.
- `VM_USER`: usuario de conexión SSH.

Esto permite que cada actualización del dataset o del pipeline (reflejada en `manifest.json`) dispare una nueva ejecución automáticamente, sin intervención manual.

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
