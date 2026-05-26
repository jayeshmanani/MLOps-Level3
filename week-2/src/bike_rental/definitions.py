from dagster import Definitions, definitions
from bike_rental.defs.assets.bike_rental import bike_rental_data


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            bike_rental_data,
        ]
    )
