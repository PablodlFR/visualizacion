from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from src import lab_renta

defs = Definitions(
    assets=load_assets_from_modules([lab_renta]),
    asset_checks=load_asset_checks_from_modules([lab_renta]),
)