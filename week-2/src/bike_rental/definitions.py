from dagster import Definitions, definitions

from bike_rental.defs.assets.bike_rental import (
    bike_rental_hourly,
    direct_pick_up_hourly,
)


@definitions
def defs() -> Definitions:
    return Definitions(assets=[bike_rental_hourly, direct_pick_up_hourly])
