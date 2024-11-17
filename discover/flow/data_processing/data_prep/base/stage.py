#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/base/stage.py                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Sunday November 17th 2024 12:55:12 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

from datetime import datetime

from discover.assets.dataset import Dataset
from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_processing.base.stage import DataProcessingStage
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PREP STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPrepStage(DataProcessingStage):
    """
    Stage for preparing data in the data pipeline.

    This class performs data preparation tasks, updating endpoints with changes
    in the source data or running tasks as needed. It inherits from DataProcessingStage
    and implements the `run` method, which handles the logic for preparing and updating
    datasets.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
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
        """Executes the data preparation stage and returns the asset ID.

        This method determines the execution path based on whether the endpoint
        exists and whether the `force` flag is set. It runs tasks to prepare
        the data or updates the endpoint if changes are detected in the source data.

        Returns:
            str: The asset ID of the prepared dataset.
        """
        source_asset_id = self._get_asset_id(config=self._source_config)
        destination_asset_id = self._get_asset_id(config=self._destination_config)
        endpoint_exists = self._dataset_exists(asset_id=destination_asset_id)

        if self._force:
            if endpoint_exists:
                self._remove_dataset(asset_id=destination_asset_id)
            self._run_task(
                source_asset_id=source_asset_id,
                destination_asset_id=destination_asset_id,
            )
        elif endpoint_exists:
            dataset_meta = self._load_dataset_metadata(asset_id=source_asset_id)
            if not dataset_meta.consumed:
                self._update_endpoint(
                    source_asset_id=source_asset_id,
                    destination_asset_id=destination_asset_id,
                )
        else:
            self._run_task(
                source_asset_id=source_asset_id,
                destination_asset_id=destination_asset_id,
            )
        return destination_asset_id

    def _update_endpoint(self, source_asset_id, destination_asset_id):
        """Updates the endpoint dataset with changes in the source dataset.

        Args:
            source_asset_id (str): The asset identifier for the input dataset.
            destination_asset_id (str): The asset identifier for the output dataset.
        """
        source_dataset = self._load_dataset(
            asset_id=source_asset_id, config=self._source_config
        )
        destination_dataset = self._load_dataset(
            asset_id=destination_asset_id, config=self._destination_config
        )
        tqd_cols = [
            col
            for col in destination_dataset.content.columns
            if col.startswith(self.stage.id) or col == "id"
        ]
        destination_dataset.content = source_dataset.content.merge(
            destination_dataset.content[tqd_cols], how="left", on="id"
        )
        self._save_dataset(dataset=destination_dataset)
        source_dataset = self._mark_consumed(dataset=source_dataset)
        self._save_dataset_metadata(dataset=source_dataset)

    def _run_task(self, source_asset_id: str, destination_asset_id: str) -> None:
        """Performs the core logic of the stage, executing tasks in sequence.

        Args:
            source_asset_id (str): The asset identifier for the source dataset.
            destination_asset_id (str): The asset identifier for the destination dataset.
        """
        source_dataset = self._load_dataset(
            asset_id=source_asset_id, config=self._source_config
        )
        data = source_dataset.content
        for task in self._tasks:
            data = task.run(data=data)
        destination_dataset = self._create_dataset(
            asset_id=destination_asset_id, config=self._destination_config, data=data
        )
        self._save_dataset(dataset=destination_dataset)
        source_dataset = self._mark_consumed(dataset=source_dataset)
        self._save_dataset_metadata(dataset=source_dataset)

    def _mark_consumed(self, dataset: Dataset) -> Dataset:
        """Marks the dataset as having been consumed.

        Args:
            dataset (Dataset): The dataset to mark as consumed.

        Returns:
            Dataset: The updated dataset marked as consumed.
        """
        dataset.consumed = True
        dataset.dt_consumed = datetime.now()
        dataset.consumed_by = self.__class__.__name__
        return dataset
