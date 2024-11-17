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
# Modified   : Sunday November 17th 2024 01:09:34 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Stage Module"""


import pandas as pd

from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_processing.base.stage import DataProcessingStage
from discover.infra.service.logging.stage import stage_logger
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class IngestStage(DataProcessingStage):
    """
    Stage for data ingestion in the data processing pipeline.

    This class handles the ingestion of raw data from a specified source, applies
    a series of tasks to preprocess the data, and saves the processed result to
    the destination. It inherits from `DataProcessingStage` and implements the
    logic for data loading and saving.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source, including the file path.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
        )

    @stage_logger
    def run(self) -> str:
        """
        Executes the data ingestion stage by loading the source dataset, applying
        tasks, and saving the processed result.

        Returns:
            str: The asset ID of the destination dataset.

        Raises:
            DatasetNotFoundError: If the source dataset cannot be loaded from the repository.
            DatasetSaveError: If there is an error saving the processed dataset.
        """
        destination_asset_id = self._get_asset_id(config=self._destination_config)
        endpoint_exists = self._dataset_exists(asset_id=destination_asset_id)

        if not self._force and endpoint_exists:
            pass
        else:
            if endpoint_exists:
                self._remove_dataset(asset_id=destination_asset_id)
            data = self._load_dataset(filepath=self._source_config.filepath)

            for task in self._tasks:
                data = task.run(data=data)

            destination_dataset = self._create_dataset(
                asset_id=destination_asset_id,
                config=self._destination_config,
                data=data,
            )
            self._save_dataset(dataset=destination_dataset)

        return destination_asset_id

    def _load_dataset(self, filepath: str) -> pd.DataFrame:
        """
        Loads a dataset from a file.

        Args:
            filepath (str): The path to the file to be loaded.

        Returns:
            pd.DataFrame: The loaded dataset as a Pandas DataFrame.

        Raises:
            FileNotFoundError: If the file is not found.
        """
        return IOService.read(filepath=filepath)
