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
        path = Path(self.base_path) / rel_path
        return pd.read_csv(path)

    def write(self, df: pd.DataFrame, rel_path: str) -> None:
        """Write a DataFrame to a CSV file at the specified path."""
        path = Path(self.base_path) / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
