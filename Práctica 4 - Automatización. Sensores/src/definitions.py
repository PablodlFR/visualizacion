from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from src import lab_renta
from src.sensor import sensor_rentas, pipeline_rentas_job

defs = Definitions(
    assets=load_assets_from_modules([lab_renta]),
    asset_checks=load_asset_checks_from_modules([lab_renta]),
    jobs=[pipeline_rentas_job],
    sensors=[sensor_rentas],
)