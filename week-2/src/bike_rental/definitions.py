"""List of all the definitions for the bike rental data."""

from dagster import Definitions, definitions

from bike_rental.defs.assets.bike_rental import (
    bike_rental_hourly,
    direct_pick_up_hourly,
    merged_hourly,
)


@definitions
def defs() -> Definitions:
    """Define the assets for the bike rental data.

    Return a Definitions object containing the assets.
    """
    return Definitions(
        assets=[
            bike_rental_hourly,
            direct_pick_up_hourly,
            merged_hourly,
        ]
    )
