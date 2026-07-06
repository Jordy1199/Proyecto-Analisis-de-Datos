from airflow.sdk import dag, task
from datetime import datetime
import sys
from pathlib import Path

sys.path.append(str(Path.home() / "favorita_pipeline" / "scripts"))

@dag(
    dag_id="favorita_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["favorita", "analisis_datos"],
)
def favorita_pipeline():

    @task
    def cargar_datos_task():
        from cargar_datos import cargar_datos
        train, stores, oil, holidays, transactions = cargar_datos()
        return {"filas_train": train.height}

    @task
    def eda_inicial_task(resultado_carga):
        from eda_inicial import eda_inicial
        eda_inicial()
        return "eda completado"

    resultado = cargar_datos_task()
    eda_inicial_task(resultado)

favorita_pipeline()
