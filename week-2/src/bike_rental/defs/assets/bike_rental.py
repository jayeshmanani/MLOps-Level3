"""Assets for the bike rental data."""

import dagster as dg

from bike_rental.defs.assets import constants
from bike_rental.defs.assets.helper import (
    data_import_csv,
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
