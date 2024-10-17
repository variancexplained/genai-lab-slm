#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/fao/location.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Wednesday October 16th 2024 11:56:28 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""FilePath Service Module"""

import os

from discover.core.flow import PhaseDef, StageDef
from discover.infra.persistence.dal.base.location import LocationService


# ------------------------------------------------------------------------------------------------ #
class FileLocationService(LocationService):
    """
    Manages the generation and reconstruction of file paths for different entities, phases, and stages
    within a workspace. It ensures that directories are created as needed and provides a consistent
    naming convention for files based on provided parameters.

    Attributes:
        _workspace (str): The base directory where all entities and related files are stored.
    """

    def __init__(self, workspace: str) -> None:
        """
        Initializes the LocationService with the specified workspace.

        Args:
            workspace (str): The base directory where entities and related files will be stored.
        """
        self._workspace = workspace

    def get_filepath(
        self,
        entity: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        partition: bool = True,
    ) -> str:
        """
        Constructs the file path for a given entity, phase, stage, and name. If the partition parameter
        is True, it appends a `.parquet` extension to the filename. Ensures that the necessary directories
        are created.

        Args:
            entity (str): The type of entity (e.g., 'dataset' or 'model') to be stored.
            phase (PhaseDef): The phase definition object representing the phase of the task.
            stage (StageDef): The stage definition object representing the stage of the task.
            name (str): The specific name or identifier for the file.
            partition (bool, optional): Whether to partition the file (adds a `.parquet` extension if True).
                                        Defaults to True.

        Returns:
            str: The complete file path for the specified entity, phase, stage, and name.
        """
        filename = (
            f"appvocai_discover-{phase.directory}-{stage.directory}-{name}-{entity}"
        )
        filename = filename if not partition else f"{filename}.parquet"
        filepath = os.path.join(self._workspace, entity, phase.directory, filename)
        directory = filepath if not partition else os.path.dirname(filepath)
        os.makedirs(directory, exist_ok=True)
        return filepath
