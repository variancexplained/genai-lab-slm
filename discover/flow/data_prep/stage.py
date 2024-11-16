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
# Modified   : Saturday November 16th 2024 02:43:59 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

import logging
from abc import abstractmethod
from typing import List, Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persistence.repo.dataset import DatasetRepo
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
        # Obtain the source and destination asset identifiers for repository access
        source_asset_id = self._get_asset_id(config=self._source_config)
        destination_asset_id = self._get_asset_id(config=self._destination_config)
        # Determine whether the endpoint already exists
        endpoint_exists = self._dataset_exists(asset_id=destination_asset_id)

        # Determine execution path
        if self._force:
            if endpoint_exists:
                self._remove_dataset(asset_id=destination_asset_id)
            self._run_task(
                source_asset_id=source_asset_id,
                destination_asset_id=destination_asset_id,
            )
        elif endpoint_exists:
            # Check if the source data has changed.
            if self._is_fresh_data(asset_id=source_asset_id):
                # Updated endpoint with fresh data.
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

    @abstractmethod
    def _update_endpoint(self, source_asset_id: str, destination_asset_id: str) -> None:
        """Updates the endpoint dataset with changes in the source dataset.

        Args:
            source_asset_id (str): The asset identifier for the input dataset.
            destination_asset_id (str): The asset identifier for the output dataset.
        """

    def _run_task(self, source_asset_id: str, destination_asset_id: str) -> None:
        """Performs the core logic of the Stage, executing tasks in sequence"""
        data = self._load_data(asset_id=source_asset_id)
        # Iterate through tasks. Default behavior is that task output is input for subsequent task
        for task in self._tasks:
            data = task.run(data=data)

        # Create the destination dataset
        dataset = self._create_dataset(
            asset_id=destination_asset_id, config=self._destination_config, data=data
        )
        self._save_dataset(dataset=dataset)

    def _dataset_exists(self, asset_id: str) -> bool:
        """Checks if the dataset exists in the repository."""
        try:
            return self._repo.exists(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} existence.\n{str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _is_fresh_data(self, asset_id: str) -> bool:
        """Returns a boolean indicating whether the data for the given asset_id is fresh.

        Args:
            asset_id (str): The asset identifier for the dataset

        Returns:
            bool: True if the dataset is fresh.
        """
        try:
            return self._repo.is_fresh_data(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} freshness: {str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _load_data(
        self, asset_id: str, config: NestedNamespace
    ) -> Union[pd.DataFrame, DataFrame]:
        """
        Loads the a dataset from the repository.

        Args:
            asset_id (str): The identifier for the dataset asset.
            config (NestedNamespace): Dataset configuration Specifies whether the load the
                dataset as a pandas or distributed  (PySpark) DataFrame with or without NLP
                dependencies.

        Returns:
            Union[pd.DataFrame, DataFrame]: Pandas or PySpark DataFrame.

        Raises:
            FileNotFoundError: If the source dataset cannot be found or loaded.
            RuntimeError: If unrecognized error occurs.
        """
        try:
            dataset = self._repo.get(
                asset_id=asset_id,
                distributed=config.distributed,
                nlp=config.nlp,
            )
            # Marks the dataset as no longer fresh acknowledging the dataset has been
            # consumed to avoid unnecessary processing in future executions when data
            # hasn't changed.
            self._repo.dataset_consumed(asset_id=asset_id)

            if config.distributed:
                # Rename the pandas index column if it exists. This is annoyance that occurs
                # when a file saved with pandas indexes is read as a PySpark DataFrame
                if "__index_level_0__" in dataset.content.columns:
                    dataset.content = dataset.content.withColumnRenamed(
                        "__index_level_0__", "pandas_index"
                    )
            return dataset.content
        except FileNotFoundError as e1:
            msg = f"Dataset {asset_id} not found in the repository.\n{e1}"
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        except Exception as e2:
            msg = f"RuntimeError encountered while attempting to load dataset {asset_id}.\n{e2}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _create_dataset(
        self,
        asset_id: str,
        config: NestedNamespace,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
    ) -> Dataset:
        """
        Creates a dataset.

        Args:
            asset_id (str): The identifier for the asset.
            config (NestedNamespace): Configuration for the dataset.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The dataset payload.

        Returns:
            Dataset: The created dataset.
        """
        return Dataset(
            phase=PhaseDef.from_value(config.phase),
            stage=DataPrepStageDef.from_value(config.stage),
            name=config.name,
            content=data,
            nlp=config.nlp,
            distributed=config.distributed,
        )

    def _save_dataset(self, dataset: Dataset) -> None:
        """
        Saves a dataset to the repository.

        Args:
            dataset (Dataset): The dataset to be saved.

        Raises:
            RuntimeError: If saving the dataset fails.
        """
        try:
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset\n{dataset} {str(e)}")

    def _remove_dataset(self, asset_id: str) -> None:
        """
        Removes the dataset with the specified asset_id from the repository.

        Args:
            asset_id (str): The asset id for the dataset to be removed.

        Raises:
            DatasetNotFoundError: If the destination dataset cannot be found.
        """
        try:
            self._repo.remove(asset_id=asset_id)
        except Exception as e:
            raise RuntimeError(
                f"Failed to remove dataset with asset_id: {asset_id}\n {str(e)}"
            )

    def _get_asset_id(self, config: NestedNamespace) -> str:
        """Returns the asset id given an asset configuration

        Args:
            config (NestedNamespace): an asset configuration

        Returns:
            str: The asset_id for the asset
        """
        return self._asset_idgen.get_asset_id(
            asset_type=config.asset_type,
            phase=PhaseDef.from_value(config.phase),
            stage=DataPrepStageDef.from_value(config.stage),
            name=config.name,
        )
