"""CSV I/O resource for reading and writing CSV files with a base path."""

from pathlib import Path

import pandas as pd
from dagster import ConfigurableResource


class CSVIO(ConfigurableResource):
    """Resource for reading and writing CSV files.

    with a configurable base path.
    """

    base_path: str = "."

    def read(self, rel_path: str, configs: dict) -> pd.DataFrame:
        """Read a CSV file from the specified path.

        and return it as a DataFrame.
        """
        try:
            path = Path(self.base_path) / rel_path
            data = pd.read_csv(path, storage_options=configs)
            return data
        except Exception as e:
            raise Exception(f"error occurred while reading CSV file: {e}")

    def write(self, df: pd.DataFrame, rel_path: str, configs: dict) -> None:
        """Write a DataFrame to a CSV file at the specified path.

        Parameters
        ----------
        df
            Dataframe to persist.
        rel_path
            Path relative to the configured base directory.
        configs
            Storage options for writing the CSV file.

        """
        try:
            path = Path(self.base_path) / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False, storage_options=configs)
        except Exception as e:
            raise Exception(f"error occurred while writing CSV file: {e}")
