"""CSV I/O resource for reading and writing CSV files with a base path."""

from pathlib import Path

import pandas as pd
from dagster import ConfigurableResource


class CSVIO(ConfigurableResource):
    """Resource for reading and writing CSV files.

    with a configurable base path.
    """

    base_path: str = "."

    def read(self, rel_path: str) -> pd.DataFrame:
        """Read a CSV file from the specified path.

        and return it as a DataFrame.
        """
        try:
            path = Path(self.base_path) / rel_path
            data = pd.read_csv(path)
            return data
        except Exception as e:
            raise Exception(f"error occurred while reading CSV file: {e}")

    def write(self, df: pd.DataFrame, rel_path: str) -> None:
        """Write a DataFrame to a CSV file at the specified path."""
        try:
            path = Path(self.base_path) / rel_path
            path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(path, index=False)
        except Exception as e:
            raise Exception(f"error occurred while writing CSV file: {e}")
