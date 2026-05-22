import os
from dagster import sensor, RunRequest, AssetSelection, define_asset_job

# JOB → ejecuta todo el pipeline
pipeline_rentas_job = define_asset_job(
    name="pipeline_rentas_job",
    selection=AssetSelection.all()
)

# SENSOR → detecta cambios en el dataset
@sensor(job=pipeline_rentas_job)
def sensor_rentas(context):
    ruta_fichero = "./data/distribucion-renta-canarias.csv"
    
    # Verificar que existe el fichero
    if not os.path.exists(ruta_fichero):
        context.log.warning(f"Fichero {ruta_fichero} no encontrado.")
        return

    # Detectar cambios (fecha de modificación)
    last_mtime = context.cursor or "0"
    curr_mtime = str(os.path.getmtime(ruta_fichero))
    
    if curr_mtime != last_mtime:
        yield RunRequest(
            run_key=curr_mtime,
            message="Cambio detectado en el dataset. Ejecutando pipeline..."
        )
        context.update_cursor(curr_mtime)