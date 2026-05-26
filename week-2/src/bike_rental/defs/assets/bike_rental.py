import dagster as dg
import pandas as pd
from bike_rental.defs.assets import constants

def data_import_csv(file_path: str) -> pd.DataFrame:
    """
    Imports the data from the given file path and returns a pandas DataFrame.
    """
    data = pd.DataFrame()
    try:
        data = pd.read_csv(file_path)
        data.head()
    except Exception as e:
        raise FileNotFoundError(f"error occured data import: {e}")
    return data

@dg.asset
def bike_rental_data() -> pd.DataFrame:
    """
    Asset that imports the bike rental data from a CSV file and returns it as a pandas DataFrame.
    """
    file_path = constants.F_BIKE_RENTALS
    data = data_import_csv(file_path)
    data.head()
    # Below is line to test if it is reading the data
    # data.to_csv(constants.F_op_path.format("bike_rental"), index=False)
    return data






