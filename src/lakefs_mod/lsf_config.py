"""LakeFS configuration management."""

import os
from typing import Any

import dotenv
from lakefs_spec import LakeFSFileSystem

dotenv.load_dotenv()


class LFSConfig:
    """LakeFS configuration management."""

    def __init__(self):
        """Initialize the configuration."""
        self.config: dict[str, Any] = {}
        self.repo = os.getenv("LAKEFS_REPOSITORY")
        self.lfs_client = None
        self.load_config()

    def load_config(self):
        """Load configuration from environment variables."""
        self.config = {
            "host": os.getenv("LAKEFS_ENDPOINT"),
            "username": os.getenv("LAKEFS_ACCESS_KEY_ID"),
            "password": os.getenv("LAKEFS_SECRET_ACCESS_KEY"),
        }
        if self.lfs_client is None:
            self.lfs_client = LakeFSFileSystem(**self.config)
            print(f"Connected successfully! Your repositories: {self.repo}")
        return self.lfs_client
