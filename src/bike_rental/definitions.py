"""Dagster definitions for the bike rental data pipeline."""

from dagster import Definitions, definitions, load_assets_from_package_module

from bike_rental.defs import assets
from bike_rental.defs.resources import CSVIO, ProjectConfig

asset_defs = load_assets_from_package_module(assets)


@definitions
def defs() -> Definitions:
    """Define the assets for the bike rental data.

    Return a Definitions object containing the assets.
    """
    return Definitions(
        assets=asset_defs,
        resources={
            "csv_io": CSVIO(base_path="."),
            "project_config": ProjectConfig(),
        },
    )
