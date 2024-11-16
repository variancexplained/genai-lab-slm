#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/stage.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Friday November 15th 2024 05:52:34 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

import logging
from typing import List, Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.persistence.repo.exception import DatasetNotFoundError
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PREP STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPrepStage(Stage):
    """
    A stage class for preparing datasets, handling loading, processing, and saving of data.

    This class orchestrates the execution of data preparation tasks, including loading source datasets,
    applying a series of tasks, and saving the processed data to a destination repository.

    Parameters
    ----------
    source_config : dict
        Configuration for the source dataset, including details like phase, stage, and name.
    destination_config : dict
        Configuration for the destination dataset, including details like phase, stage, and name.
    tasks : List[Task]
        A list of tasks to execute as part of the data preparation stage.
    force : bool, optional
        Whether to force execution if the destination dataset endpoint already exists (default is False).
    repo : DatasetRepo, optional
        A repository for dataset persistence, injected via dependency injection (default is `DiscoverContainer.repo.dataset_repo`).
    **kwargs : dict
        Additional keyword arguments for stage configuration.

    Attributes
    ----------
    _repo : DatasetRepo
        The repository instance used for dataset persistence.
    _source_asset_id : str
        The generated asset ID for the source dataset based on the configuration.
    _destination_asset_id : str
        The generated asset ID for the destination dataset based on the configuration.
    _logger : logging.Logger
        Logger instance for logging events related to the data preparation stage.

    Methods
    -------
    run() -> str
        Executes the stage by loading the source dataset, applying tasks, and saving the result.
    _create_destination_dataset(data: Union[pd.DataFrame, pyspark.sql.DataFrame]) -> Dataset
        Creates the destination dataset with the processed data and configuration details.
    _load_source_dataset() -> Dataset
        Loads the source dataset from the repository using the source asset ID.
    _save_destination_dataset(dataset: Dataset) -> None
        Saves the processed dataset to the repository using the destination asset ID.
    _endpoint_exists(asset_id: str) -> bool
        Checks if the dataset endpoint already exists in the repository.

    Notes
    -----
    The `DataPrepStage` class leverages dependency injection to retrieve a dataset repository instance.
    It ensures that datasets are properly loaded and saved based on the specified configurations.
    """

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the data preparation stage with source and destination configurations, tasks, and optional parameters.

        Args:
            source_config (dict): Configuration details for the source dataset.
            destination_config (dict): Configuration details for the destination dataset.
            tasks (List[Task]): List of tasks to run during this stage.
            force (bool, optional): If True, forces execution even if the destination dataset exists (default is False).
            repo (DatasetRepo, optional): Repository for dataset persistence, injected via dependency injection.
        """
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )
        self._asset_idgen = asset_idgen
        self._repo = repo

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the destination dataset."""
        return PhaseDef.from_value(value=self._destination_config.phase)

    @property
    def stage(self) -> PhaseDef:
        """Returns the stage of the destination dataset."""
        return DataPrepStageDef.from_value(value=self._destination_config.stage)

    @stage_logger
    def run(self) -> str:
        """
        Executes the data preparation stage by loading the source dataset, applying tasks, and saving the processed result.

        Args:
            None

        Returns:
            str: The asset ID of the destination dataset.

        Raises:
            DatasetNotFoundError: If the source dataset cannot be loaded from the repository.
            DatasetSaveError: If there is an error saving the processed dataset.
        """
        try:
            self._destination_asset_id = self._asset_idgen.get_asset_id(
                asset_type=self._destination_config.asset_type,
                phase=PhaseDef.from_value(self._destination_config.phase),
                stage=DataPrepStageDef.from_value(self._destination_config.stage),
                name=self._destination_config.name,
            )
            if (
                self._endpoint_exists(asset_id=self._destination_asset_id)
                and not self._force
            ):
                return self._destination_asset_id
            else:
                if self._repo.exists(asset_id=self._destination_asset_id):
                    self._repo.remove(asset_id=self._destination_asset_id)

                data = self._load_source_data()

                for task in self._tasks:
                    data = task.run(data=data)

                dataset = self._create_destination_dataset(data=data)
                self._save_destination_dataset(dataset=dataset)

                return self._destination_asset_id
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Source dataset not found: {str(e)}")
        except Exception as e:
            raise Exception(f"Failed to save processed dataset: {str(e)}")

    def _endpoint_exists(self, asset_id: str) -> bool:
        """Checks if the dataset endpoint already exists in the repository."""
        try:
            return self._repo.exists(asset_id=asset_id)
        except Exception as e:
            self._logger.error(f"Error checking endpoint existence: {str(e)}")
            raise RuntimeError("Error checking if the dataset endpoint exists")

    def _load_source_data(self) -> pd.DataFrame:
        """
        Loads the source dataset from the repository using the source asset ID.

        Returns:
            pd.DataFrame: The loaded source dataset.

        Raises:
            DatasetNotFoundError: If the source dataset cannot be found or loaded.
        """
        try:
            source_asset_id = AssetIDGen.get_asset_id(
                asset_type=self._source_config.asset_type,
                phase=PhaseDef.from_value(self._source_config.phase),
                stage=DataPrepStageDef.from_value(self._source_config.stage),
                name=self._source_config.name,
            )
            dataset = self._repo.get(
                asset_id=source_asset_id,
                distributed=self._source_config.distributed,
                nlp=self._source_config.nlp,
            )

            if self._source_config.distributed:
                # Rename the pandas index column if it exists
                if "__index_level_0__" in dataset.content.columns:
                    dataset.content = dataset.content.withColumnRenamed(
                        "__index_level_0__", "pandas_index"
                    )
            return dataset.content
        except FileNotFoundError as e:
            raise DatasetNotFoundError(f"Source dataset not found: {str(e)}")

    def _create_destination_dataset(
        self, data: Union[pd.DataFrame, pyspark.sql.DataFrame]
    ) -> Dataset:
        """
        Creates the destination dataset with the processed data and configuration details.

        Args:
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The processed data to be saved.

        Returns:
            Dataset: The destination dataset to be saved.
        """
        return Dataset(
            phase=PhaseDef.from_value(self._destination_config.phase),
            stage=DataPrepStageDef.from_value(self._destination_config.stage),
            name=self._destination_config.name,
            content=data,
            nlp=self._destination_config.nlp,
            distributed=self._destination_config.distributed,
        )

    def _save_destination_dataset(self, dataset: Dataset) -> None:
        """
        Saves the processed dataset to the repository using the destination asset ID.

        Args:
            dataset (Dataset): The dataset to be saved.

        Raises:
            DatasetSaveError: If saving the dataset fails.
        """
        try:
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise Exception(f"Failed to save dataset: {str(e)}")

    def _remove_destination_dataset(self) -> None:
        """
        Removes the destination dataset from the repository.

        Raises:
            DatasetNotFoundError: If the destination dataset cannot be found.
        """
        try:
            self._repo.remove(asset_id=self._destination_asset_id)
        except Exception as e:
            raise DatasetNotFoundError(f"Failed to remove dataset: {str(e)}")
