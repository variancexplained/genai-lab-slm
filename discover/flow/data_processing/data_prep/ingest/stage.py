#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/ingest/stage.py                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:57:59 pm                                              #
# Modified   : Saturday November 16th 2024 05:46:10 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Stage Module"""

import logging
from typing import List

import pandas as pd

from discover.flow.base.task import Task
from discover.flow.data_processing.base.stage import DataProcessingStage
from discover.infra.service.logging.stage import stage_logger
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class IngestStage(DataProcessingStage):

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @stage_logger
    def run(self) -> str:
        """
        Executes the data ingestion stage by loading the source dataset, applying tasks, and saving the processed result.

        Args:
            None

        Returns:
            str: The asset ID of the destination dataset.

        Raises:
            DatasetNotFoundError: If the source dataset cannot be loaded from the repository.
            DatasetSaveError: If there is an error saving the processed dataset.
        """
        # Obtain the destination asset identifiers for repository access
        destination_asset_id = self._get_asset_id(config=self._destination_config)
        # Determine whether the endpoint already exists
        endpoint_exists = self._dataset_exists(asset_id=destination_asset_id)

        # If not forcing execution and endpoint exists, do nothing
        if not self._force and endpoint_exists:
            pass
        else:
            if endpoint_exists:
                self._remove_dataset(asset_id=destination_asset_id)
            data = self._load_dataset(filepath=self._source_config.filepath)

            for task in self._tasks:
                data = task.run(data=data)

            # Create the destination dataset
            destination_dataset = self._create_dataset(
                asset_id=destination_asset_id,
                config=self._destination_config,
                data=data,
            )
            self._save_dataset(dataset=destination_dataset)

        return destination_asset_id

    def _load_dataset(self, filepath: str) -> pd.DataFrame:
        """
        Loads the a dataset from file

        Args:
            filepath (str): The path to the file to be loaded
        Returns:
            pd.DataFrame
        Raises:
            FileNotFoundError if the file is not found.
        """
        return IOService.read(filepath=filepath)
