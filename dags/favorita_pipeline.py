from airflow.sdk import dag, task
from datetime import datetime, timedelta
import sys
import logging
from pathlib import Path

sys.path.append(str(Path.home() / "favorita_pipeline" / "scripts"))

logger = logging.getLogger("airflow.task")


def registrar_error(context):
    tarea = context.get("task_instance")
    logger.error(
        f"FALLO en tarea '{tarea.task_id}' del DAG '{tarea.dag_id}' "
        f"- run_id: {tarea.run_id} - intento: {tarea.try_number}"
    )


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": registrar_error,
}


@dag(
    dag_id="favorita_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["favorita", "analisis_datos"],
    default_args=default_args,
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

    @task
    def limpiar_datos_task(resultado_eda):
        from limpiar_datos import limpiar_datos
        limpiar_datos()
        return "limpieza completada"

    @task
    def consolidar_datos_task(resultado_limpieza):
        from consolidar import consolidar
        df = consolidar()
        return {"filas_consolidado": df.height}

    @task
    def eda_profundo_task(resultado_consolidado):
        from eda_profundo import eda_profundo
        eda_profundo()
        return "eda profundo completado"

    resultado_carga = cargar_datos_task()
    resultado_eda = eda_inicial_task(resultado_carga)
    resultado_limpieza = limpiar_datos_task(resultado_eda)
    resultado_consolidado = consolidar_datos_task(resultado_limpieza)
    resultado_eda_profundo = eda_profundo_task(resultado_consolidado)


favorita_pipeline()
