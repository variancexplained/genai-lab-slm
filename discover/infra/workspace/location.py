#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/location.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:53:18 am                                               #
# Modified   : Monday December 30th 2024 04:13:34 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""file Location Module"""


# ------------------------------------------------------------------------------------------------ #
from pathlib import Path
from typing import Dict

from discover.asset.base.atype import AssetType
from discover.asset.dataset import FileFormat
from discover.core.flow import PhaseDef


class LocationService:
    def __init__(self, config: Dict) -> None:
        """
        Initializes the LocationService with workspace configuration.

        Args:
            config (dict): Workspace configuration dictionary. Must contain:
                - "location": Base workspace location.
                - "files": Relative path to the files directory within the workspace.

        Raises:
            KeyError: If required keys ("location", "files") are missing in the config.
            ValueError: If any config values are empty or invalid paths.
        """
        # Validate required keys in the config
        if "location" not in config or "files" not in config:
            raise KeyError("Config must contain 'location' and 'files' keys.")

        workspace_location = config["location"]
        files_location = config["files"]

        # Validate non-empty and valid paths
        if not workspace_location or not files_location:
            raise ValueError("'location' and 'files' values must be non-empty.")

        self._workspace_location = Path(workspace_location)
        self._files_location = self._workspace_location / files_location

    def get_filepath(
        self,
        asset_type: AssetType,
        asset_id: str,
        phase: PhaseDef,
        file_format: FileFormat,
    ) -> str:
        """
        Constructs a file path based on the asset type, asset ID, phase, and file format.

        Args:
            asset_type (AssetType): The type of asset (e.g., dataset, model).
            asset_id (str): The unique identifier for the asset.
            phase (PhaseDef): The phase associated with the asset.
            file_format (FileFormat): The desired file format.

        Returns:
            str: The constructed file path as a string.

        Raises:
            ValueError: If any required arguments are missing or invalid.
        """
        # Validate input arguments
        if not all([asset_type, asset_id, phase, file_format]):
            raise ValueError(
                "All arguments (asset_type, asset_id, phase, file_format) must be provided and non-empty."
            )

        # Ensure valid asset_type and file_format enums
        if not hasattr(asset_type, "value") or not hasattr(file_format, "value"):
            raise ValueError(
                "asset_type and file_format must be valid enums with a 'value' attribute."
            )

        # Construct the file extension and filename
        filext = file_format.value.lstrip(".")  # Remove leading dot if present
        filename = f"{asset_id}.{filext}"

        # Use Path to construct the full file path
        full_path = self._files_location / phase.value / asset_type.value / filename
        return str(full_path)  # Convert Path object to string
