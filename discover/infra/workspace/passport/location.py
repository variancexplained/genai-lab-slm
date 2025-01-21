#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/passport/location.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:53:18 am                                               #
# Modified   : Tuesday January 21st 2025 05:08:56 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""file Location Module"""

from pathlib import Path

from discover.core.flow import PhaseDef
from discover.infra.utils.file.fileset import FileFormat

# ------------------------------------------------------------------------------------------------ #


class LocationService:
    """Service responsible for the structure and organization of files in the workspace.

    Args:
        file_location (str): File base directory in the workspace.
    """

    def __init__(self, files_location: str) -> None:
        self._files_location = Path(files_location)

    def get_filepath(
        self,
        asset_id: str,
        phase: PhaseDef,
        file_format: FileFormat,
    ) -> str:
        """
        Constructs a file path based on the asset type, asset ID, phase, and file format.

        Args:
            asset_id (str): The unique identifier for the asset.
            phase (PhaseDef): The phase associated with the asset.
            file_format (FileFormat): The desired file format.

        Returns:
            str: The constructed file path as a string.

        Raises:
            TypeError: If any required arguments are missing or invalid.
            Exception: If an unknown exception occurs.
        """
        try:
            # Construct the file extension and filename
            filext = file_format.ext.lstrip(".")  # Remove leading dot if present
            filename = f"{asset_id}.{filext}"

            # Use Path to construct the full file path
            full_path = Path(self._files_location) / phase.value / filename

            return str(full_path)  # Convert Path object to string
        except AttributeError as e:
            msg = "An TypeError occurred while creating the filepath."
            msg += f"asset_id: Expected a string type, received a {type(asset_id)} object.\n"
            msg += (
                f"phase: Expected a PhaseDef type, received a {type(phase)} object.\n"
            )
            msg += f"file_format: Expected a FileFormat type, received a {type(file_format)} object.\n{e}"
            raise TypeError(msg)
        except Exception as e:
            msg = f"Un expected exception occurred while creating filepath.\n{e}"
