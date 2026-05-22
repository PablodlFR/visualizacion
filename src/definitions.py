from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
import proyecto

defs = Definitions(
    assets=load_assets_from_modules([proyecto]),
    asset_checks=load_asset_checks_from_modules([proyecto]),
)