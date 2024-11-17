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
# Modified   : Saturday November 16th 2024 07:12:01 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

from abc import abstractmethod
from datetime import datetime
from typing import List, Type, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.core.namespace import NestedNamespace
from discover.flow.base.task import Task
from discover.flow.data_processing.base.stage import DataProcessingStage
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PREP STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPrepStage(DataProcessingStage):
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

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
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

    @property
    @abstractmethod
    def stage_prefix(self) -> str:
        """Returns the stage prefix, a prefix for each column created by tasks in this stage."""

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
        elif endpoint_exists:  # and not forcing execution
            # Check if the source data has already been consumed
            if not self._is_consumed(asset_id=source_asset_id):
                # Updated endpoint with fresh data.
                self._update_endpoint(
                    source_asset_id=source_asset_id,
                    destination_asset_id=destination_asset_id,
                )  # Stage specific

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

        # Obtain the source and destination datasets
        source_dataset = self._load_dataset(asset_id=source_asset_id)
        destination_dataset = self._load_dataset(asset_id=destination_asset_id)
        # Extract the id column and columns created by this process and added to the destination dataset
        # prefixed by the stage that created the column.
        tqd_cols = [
            col
            for col in destination_dataset.content.columns
            if col.startswith(self.stage_prefix) or col == "id"
        ]
        # Update the destination dataset with updated source dataset content
        destination_dataset.content = source_dataset.content.merge(
            destination_dataset.content[tqd_cols], how="left", on="id"
        )
        # Save the destination dataset.
        self._save_dataset(dataset=destination_dataset)
        # Mark the source dataset metadata as consumed.
        source_dataset = self._consumed(dataset=source_dataset)
        # Save the source dataset metadata
        self._save_dataset_metadata(dataset=source_dataset)

    def _run_task(self, source_asset_id: str, destination_asset_id: str) -> None:
        """Performs the core logic of the Stage, executing tasks in sequence"""
        # Obtain the source dataset
        source_dataset = self._load_dataset(asset_id=source_asset_id)
        # Extract the payload
        data = source_dataset.content
        # Iterate through tasks. Default behavior is that task output is input for subsequent task
        for task in self._tasks:
            data = task.run(data=data)

        # Create the destination dataset
        destination_dataset = self._create_dataset(
            asset_id=destination_asset_id, config=self._destination_config, data=data
        )
        self._save_dataset(dataset=destination_dataset)

        # Mark the source dataset as having been consumed.
        source_dataset = self._consumed(dataset=source_dataset)
        # Save the source dataset metadata only. We can do this without integrity errors as
        # the dataset content is immutable.
        self._save_dataset_metadata(dataset=source_dataset)

    def _is_consumed(self, asset_id: str) -> bool:
        """Returns a boolean indicating whether the data for the given asset_id is fresh.

        Args:
            asset_id (str): The asset identifier for the dataset

        Returns:
            bool: True if the dataset is fresh.
        """
        try:
            return self._repo.is_consumed(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} freshness: {str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _consumed(self, dataset: Dataset) -> Dataset:
        """Marks the dataset as having been consumed"""
        dataset.consumed = True
        dataset.dt_consumed = datetime.now()
        dataset.consumed_by = self.__class__.__name__
        return dataset

    def _load_dataset(
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
            return dataset
        except FileNotFoundError as e1:
            msg = f"Dataset {asset_id} not found in the repository.\n{e1}"
            self._logger.error(msg)
            raise FileNotFoundError(msg)
        except Exception as e2:
            msg = f"RuntimeError encountered while attempting to load dataset {asset_id}.\n{e2}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _save_dataset_metadata(self, dataset: Dataset) -> None:
        """
        Saves a dataset metadata to the repository.

        Args:
            dataset (Dataset): The dataset to be saved.

        Raises:
            RuntimeError: If saving the dataset fails.
        """
        try:
            self._repo.update_dataset_metadata(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset metadata\n{dataset}\n{str(e)}")
