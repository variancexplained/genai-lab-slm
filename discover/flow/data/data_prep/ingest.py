#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data/data_prep/ingest.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:57:59 pm                                              #
# Modified   : Wednesday December 25th 2024 07:32:22 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Stage Module"""


from discover.core.flow import DataPrepStageEnum, PhaseEnum
from discover.flow.stage.base import DataPrepStage
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
class IngestionStage(DataPrepStage):
    """
    Stage for data ingestion in the data processing pipeline.

    This class handles the ingestion of raw data from a specified source, applies
    a series of tasks to preprocess the data, and saves the processed result to
    the destination. It inherits from `DataPrepStageEnum` and implements the
    logic for data loading and saving.

    Args:
        phase (PhaseEnum): The phase of the data pipeline.
        stage (DataPrepStageEnum): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source, including the file path.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
    """

    def __init__(
        self,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
            **kwargs,
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

        if self._force or not endpoint_exists:
            data = self._load_file(filepath=self._source_config.filepath)

            for task in self._tasks:
                data = task.run(data=data)

            destination_dataset = self._create_dataset(
                asset_id=destination_asset_id,
                config=self._destination_config,
                data=data,
            )
            # Persist pipeline stage destination dataset
            self._save_dataset(dataset=destination_dataset, replace_if_exists=True)

        return destination_asset_id
