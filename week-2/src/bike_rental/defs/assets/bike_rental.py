"""Assets for the bike rental data."""

import dagster as dg

from bike_rental.defs.assets import constants
from bike_rental.defs.assets.helper import (
    data_import_csv,
    data_merger,
    data_store_csv,
    data_to_hourly,
)


@dg.asset
def bike_rental_hourly() -> None:
    """Asset that imports the bike rental dat.

    It reads a CSV file and converts it to hourly data, and
    store it in the raw folder.
    """
    data = data_import_csv(constants.F_BIKE_RENTALS)
    data = data_to_hourly(data, "datetime")
    data_store_csv(data, constants.F_raw_path.format("bike_rental_hourly"))
    return


@dg.asset
def direct_pick_up_hourly() -> None:
    """Asset that imports the direct pick up bike rental data.

    It reads a CSV file, converts it to hourly data, and stores it
    in the raw folder.
    """
    data = data_import_csv(constants.F_BIKE_RENTALS_DIRECT_PICKUP)
    data = data_to_hourly(data, "datetime")
    data_store_csv(data, constants.F_raw_path.format("direct_pick_up_hourly"))
    return


@dg.asset(deps=["direct_pick_up_hourly", "bike_rental_hourly"])
def merged_hourly() -> None:
    """Merge bike rental hourly data and direct pick up hourly data.

    It reads the two hourly data CSV files, merges them, and stores the
    merged data in the raw folder.
    """
    data1 = data_import_csv(constants.F_raw_path.format("bike_rental_hourly"))
    data2 = data_import_csv(
        constants.F_raw_path.format("direct_pick_up_hourly")
    )
    merged_data = data_merger(
        data1,
        data2,
        on_cols=["datetime", "location_id"],
        how_to="outer",
        suffixe_str=("_rentals", "_pickups"),
    )
    merged_hourly = merged_data.fillna(0)
    data_store_csv(merged_hourly, constants.F_raw_path.format("merged_hourly"))
    return
