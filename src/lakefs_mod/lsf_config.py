"""LakeFS configuration management."""

import os

import dotenv
import lakefs
import pandas as pd
from lakefs.client import Client
from lakefs_spec import LakeFSFileSystem

dotenv.load_dotenv()


class LFSConfig:
    """LakeFS configuration management optimized for MLOps orchestration."""

    _instance = None

    def __new__(cls):
        """Implement singleton pattern to ensure a single shared instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self):
        required = [
            "LAKEFS_ENDPOINT",
            "LAKEFS_ACCESS_KEY_ID",
            "LAKEFS_SECRET_ACCESS_KEY",
            "LAKEFS_REPOSITORY",
        ]

        missing = [v for v in required if not os.getenv(v)]
        if missing:
            raise OSError(f"Missing env vars: {missing}")

        self.repo_name = os.getenv("LAKEFS_REPOSITORY")
        self.branch = os.getenv("LAKEFS_BRANCH", "main")

        self.storage_options = {
            "host": os.getenv("LAKEFS_ENDPOINT"),
            "username": os.getenv("LAKEFS_ACCESS_KEY_ID"),
            "password": os.getenv("LAKEFS_SECRET_ACCESS_KEY"),
        }

        self.fs = LakeFSFileSystem(
            **self.storage_options, create_branch_ok=True, source_branch="main"
        )

        self._lakefs_client = Client(**self.storage_options)

        self.repo = lakefs.repository(
            self.repo_name, client=self._lakefs_client
        )

    def _path(self, branch: str, f_path: str) -> str:
        return f"lakefs://{self.repo_name}/{branch}/{f_path}"

    def read_csv(self, f_path: str, branch: str):
        """Read a CSV file from lakeFS using the fsspec client."""
        return pd.read_csv(
            self._path(branch, f_path),
            storage_options=self.storage_options,
        )

    def write_csv(self, df, f_path: str, branch: str):
        """Write a DataFrame to a CSV file in lakeFS using the fsspec client."""
        df.to_csv(
            self._path(branch, f_path),
            storage_options=self.storage_options,
            index=False,
        )

    @property
    def client(self):
        """Direct filesystem access (lakefs_spec client)."""
        return self.fs

    @property
    def fs_client(self):
        """Alias for direct filesystem access (lakefs_spec client)."""
        return self.fs

    # -------------------------
    # High-Level SDK Operations
    # -------------------------

    def create_branch(self, branch_name: str, source: str = "main"):
        """Create a new branch using the high-level SDK."""
        branch_obj = self.repo.branch(branch_name).create(
            source_reference=source
        )
        return branch_obj

    def diff(self, left: str, right: str = "main"):
        """Return a list of differences between branches."""
        return list(
            self.repo.branch(left).diff(other_ref=self.repo.branch(right))
        )

    def commit(self, branch: str, message: str, metadata: dict = None):
        """Commit changes with optional MLOps metadata tracking."""
        commit_metadata = metadata if metadata else {}

        commit_ref = self.repo.branch(branch).commit(
            message=message, metadata=commit_metadata
        )
        return commit_ref

    def get_uncommitted(self, branch: str):
        """Return a list of uncommitted changes on a specific branch."""
        return list(self.repo.branch(branch).uncommitted())

    def commit_if_changed(
        self,
        branch: str,
        message: str = "dagster update",
        metadata: dict = None,
    ):
        """Evaluate uncommitted changes and commits only if data change."""
        uncommitted_list = self.get_uncommitted(branch)

        if not uncommitted_list:
            print(f"No changes detected on '{branch}', skipping commit.")
            return False

        commit_ref = self.commit(branch, message, metadata=metadata)

        print(
            f"Committed {len(uncommitted_list)} changes.\
                  Commit ID: {commit_ref.id}"
        )
        return commit_ref

    def get_asset_branch(self, asset_name: str, run_id: str):
        """Generate a unique branch name on the asset name and run ID."""
        return f"dg-{asset_name}-{run_id}"

    def merge_to_main(self, branch: str):
        """Merge a feature/run branch back into main."""
        return self.repo.branch(branch).merge_into(self.repo.branch("main"))

    def read_run_data(
        self, f_path: str, asset_name: str, run_id: str
    ) -> pd.DataFrame:
        """Attempt to read from the run-specific branch, falls back to main."""
        target_branch = self.get_asset_branch(asset_name, run_id)
        try:
            print(f"Attempting to read {f_path} from branch: {target_branch}")
            return self.read_csv(f_path, branch=target_branch)
        except Exception:
            print(
                f"Branch '{target_branch}' not found. Falling back to 'main'."
            )
            return self.read_csv(f_path, branch="main")
