#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/base/stage.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Wednesday November 20th 2024 07:28:53 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

import logging
from abc import abstractmethod
from datetime import datetime
from enum import Enum
from typing import Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject
from git import Optional
from pyspark.sql import DataFrame

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.stage import Stage
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                              DATA PROCESSING STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataProcessingStage(Stage):
    """
    Base class for all data processing stages in the data pipeline.

    This class provides methods for creating, loading, saving, and removing
    datasets in a centralized or distributed repository. It includes mechanisms
    for handling dataset metadata and checking dataset existence in the repository.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        asset_idgen (Type[AssetIDGen], optional): Class used for generating asset IDs.
            Defaults to AssetIDGen.
        repo (DatasetRepo, optional): The repository object for dataset operations.
            Defaults to the dataset repository from DiscoverContainer.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
        return_dataset (bool): Whether to return the resultant dataset or the asset_id
    """

    @inject
    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        force: bool = False,
        return_dataset: bool = False,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
        )
        self._return_dataset = return_dataset
        self._asset_idgen = asset_idgen
        self._repo = repo
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    def run(self) -> str:
        """Returns the dataset asset_id."""

    def _dataset_exists(self, asset_id: str) -> bool:
        """Checks if the dataset exists in the repository.

        Args:
            asset_id (str): The identifier for the dataset.

        Returns:
            bool: True if the dataset exists, False otherwise.

        Raises:
            RuntimeError: If an error occurs while checking for dataset existence.
        """
        try:
            return self._repo.exists(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} existence.\n{str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _create_dataset(
        self,
        asset_id: str,
        config: NestedNamespace,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
    ) -> Dataset:
        """Creates a dataset.

        Args:
            asset_id (str): The identifier for the asset.
            config (NestedNamespace): Configuration for the dataset.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The dataset payload.

        Returns:
            Dataset: The created dataset.
        """
        return Dataset(
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
            content=data,
            nlp=config.nlp,
            distributed=config.distributed,
        )

    def _load_dataset(
        self, asset_id: str, config: NestedNamespace
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """Loads a dataset from the repository.

        Args:
            asset_id (str): The identifier for the dataset asset.
            config (NestedNamespace): Dataset configuration specifying whether to
                load the dataset as a Pandas or PySpark DataFrame and if NLP dependencies are needed.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: Pandas or PySpark DataFrame.

        Raises:
            FileNotFoundError: If the source dataset cannot be found or loaded.
            RuntimeError: If an unrecognized error occurs during loading.
        """
        try:
            dataset = self._repo.get(
                asset_id=asset_id,
                distributed=config.distributed,
                nlp=config.nlp,
            )

            if config.distributed and "__index_level_0__" in dataset.content.columns:
                # Rename the pandas index column if it exists in the PySpark DataFrame
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

    def _load_dataset_metadata(self, asset_id: str) -> bool:
        """Loads the metadata for a dataset.

        Args:
            asset_id (str): The identifier for the dataset.

        Returns:
            bool: Metadata of the dataset if found.

        Raises:
            RuntimeError: If an error occurs while loading the metadata.
        """
        try:
            return self._repo.get_dataset_metadata(asset_id=asset_id)
        except Exception as e:
            msg = f"Error reading dataset {asset_id} metadata.\n {str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _save_dataset(self, dataset: Dataset, replace_if_exists: bool = False) -> None:
        """Saves a dataset to the repository.

        Args:
            dataset (Dataset): The dataset to be saved.

        Raises:
            RuntimeError: If saving the dataset fails.
        """
        try:
            if self._repo.exists(asset_id=dataset.asset_id) and replace_if_exists:
                self._repo.remove(asset_id=dataset.asset_id)
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset\n{dataset} {str(e)}")

    def _save_dataset_metadata(self, dataset: Dataset) -> None:
        """Saves dataset metadata to the repository.

        Args:
            dataset (Dataset): The dataset whose metadata is to be saved.

        Raises:
            RuntimeError: If saving the metadata fails.
        """
        try:
            self._repo.update_dataset_metadata(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset metadata\n{dataset}\n{str(e)}")

    def _remove_dataset(self, asset_id: str) -> None:
        """Removes the dataset with the specified asset_id from the repository.

        Args:
            asset_id (str): The asset ID for the dataset to be removed.

        Raises:
            RuntimeError: If removing the dataset fails.
        """
        try:
            self._repo.remove(asset_id=asset_id)
        except Exception as e:
            raise RuntimeError(
                f"Failed to remove dataset with asset_id: {asset_id}\n {str(e)}"
            )

    def _get_return_value(
        self,
        dataset: Optional[Dataset] = None,
        asset_id: Optional[str] = None,
        config: Optional[NestedNamespace] = None,
        return_dataset: bool = False,
    ) -> Union[Dataset, str]:
        """Obtains and returns a value based on the boolean `return_dataset` instance variable."""
        return_dataset = return_dataset or self._return_dataset
        config = config or self._destination_config  # Config for the returned dataset.

        if return_dataset:
            try:
                return dataset or self._load_dataset(asset_id=asset_id, config=config)
            except Exception as e:
                msg = f"Unable to return dataset. The dataset or a valid asset_id and configuration must be provided.\n{e}"
                raise ValueError(msg)
        else:
            try:
                return asset_id or dataset.asset_id
            except Exception as e:
                msg = f"Unable to return an asset_id. Either the  asset_id or a dataset with an asset_id must be provided.\n{e}"
                raise ValueError(msg)


# ------------------------------------------------------------------------------------------------ #
#                                     EXECUTION PATH                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPrepExecutionPath(Enum):
    RUN = "run"
    UPDATE_ENDPOINT = "update_endpoint"
    GET_RETURN_VALUE = "get_return_value"


# ------------------------------------------------------------------------------------------------ #
#                                    DATA PREP STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataPrepStage(DataProcessingStage):
    """
    Stage for preparing data in the data processing pipeline.

    This class orchestrates the data preparation process, running a series of tasks
    on the source data, updating endpoints if necessary, and saving the processed
    data to the destination. It includes methods for merging data, executing tasks,
    and managing dataset metadata to ensure data integrity and proper lineage tracking.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
        return_dataset (bool): Whether to return the resultant dataset or the asset_id
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        return_dataset: bool = True,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
            return_dataset=return_dataset,
        )

    @stage_logger
    def run(self) -> Dataset:
        """Executes the data preparation stage and returns the asset ID.

        This method determines the execution path based on whether the endpoint
        exists and whether the `force` flag is set. It runs tasks to prepare
        the data or updates the endpoint if changes are detected in the source data.

        Returns:
            str: The asset ID of the prepared dataset.
        """
        source_asset_id = self._get_asset_id(config=self._source_config)
        destination_asset_id = self._get_asset_id(config=self._destination_config)

        # Determine execution path
        execution_path = self._get_execution_path(
            source_asset_id=source_asset_id, destination_asset_id=destination_asset_id
        )

        # Direct the process
        if execution_path == DataPrepExecutionPath.RUN:
            self._logger.debug("Data prep execution path: RUN")
            dataset = self._run(
                source_asset_id=source_asset_id,
                destination_asset_id=destination_asset_id,
            )
            return self._get_return_value(dataset=dataset)

        elif execution_path == DataPrepExecutionPath.UPDATE_ENDPOINT:
            self._logger.debug("Data prep execution path: UPDATE ENDPOINT")
            dataset = self._update_endpoint(
                source_asset_id=source_asset_id,
                destination_asset_id=destination_asset_id,
            )
            return self._get_return_value(dataset=dataset)

        else:
            self._logger.debug("Data prep execution path: RETURN")
            return self._get_return_value(
                asset_id=destination_asset_id, config=self._destination_config
            )

    def _get_execution_path(
        self, source_asset_id: str, destination_asset_id: str
    ) -> DataPrepExecutionPath:
        """Determines the data prep execution path

        There are thee binary variables creating 2^8 possible conditions that resolve to
        one of three actions:
            1. Run The pipeline and return
            2. Update the existing endpoint with new source data and return
            3. Get the return value requested. No action required.

        Option 1 executes if Force is true or the endpoint doesn't exist.
        Option 2 executes if not forcing, the endpoint exists, but there is new source data.
        Option 3 executes if not forcing, the endpoint exists and the source  has
            been consumed.

        Args:
            source_asset_id (str): Asset identifier for source dataset
            destination_asset_id (str): Asset identifier for destination dataset

        Note: Force must be set to True if the source code has changed or the output changes with
        the input, and the input has changed.
        """
        # Obtain current state
        endpoint_exists = self._dataset_exists(asset_id=destination_asset_id)
        source_consumed = self._source_consumed(asset_id=source_asset_id)

        # Easy case first
        if not self._force and source_consumed and endpoint_exists:
            return DataPrepExecutionPath.GET_RETURN_VALUE
        # Update endpoint if the soure has changed
        elif not self._force and not source_consumed and endpoint_exists:
            return DataPrepExecutionPath.UPDATE_ENDPOINT
        # Otherwise run
        else:
            return DataPrepExecutionPath.RUN

    def _run(self, source_asset_id: str, destination_asset_id: str) -> Dataset:
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

        self._save_dataset(dataset=destination_dataset, replace_if_exists=True)
        self._mark_source_consumed(asset_id=source_asset_id)

        return destination_dataset

    def _update_endpoint(self, source_asset_id, destination_asset_id) -> Dataset:
        """Updates the endpoint dataset with changes in the source dataset.

        Args:
            source_asset_id (str): The asset identifier for the input dataset.
            destination_asset_id (str): The asset identifier for the output dataset.

        Returns:
            Dataset: The updated dataset endpoint
        """
        source_dataset = self._load_dataset(
            asset_id=source_asset_id, config=self._source_config
        )
        destination_dataset = self._load_dataset(
            asset_id=destination_asset_id, config=self._destination_config
        )
        result_cols = [
            col
            for col in destination_dataset.content.columns
            if col.startswith(self.stage.id) or col == "id"
        ]
        df = self._merge_dataframes(
            source=source_dataset.content,
            destination=destination_dataset.content,
            result_cols=result_cols,
        )

        # Create the new destination dataset
        destination_dataset_updated = self._create_dataset(
            destination_asset_id, config=self._destination_config, data=df
        )
        # Save the updated dataset
        self._save_dataset(dataset=destination_dataset_updated, replace_if_exists=True)
        # Mark the source dataset as having been consumed.
        source_dataset = self._mark_source_consumed(dataset=source_dataset)
        # Persist the source dataset metadata
        self._save_dataset_metadata(dataset=source_dataset)
        return destination_dataset_updated

    def _source_consumed(self, asset_id: str) -> bool:
        """Returns True of the source has been consumed"""
        dataset_meta = self._load_dataset_metadata(asset_id=asset_id)
        return dataset_meta.consumed

    def _mark_source_consumed(self, asset_id: str) -> None:
        """Marks the dataset as having been consumed.

        Args:
            dataset (Dataset): The dataset to mark as consumed.

        Returns:
            Dataset: The updated dataset marked as consumed.
        """
        dataset = self._load_dataset_metadata(asset_id=asset_id)
        dataset.consumed = True
        dataset.dt_consumed = datetime.now()
        dataset.consumed_by = self.__class__.__name__
        self._save_dataset_metadata(dataset=dataset)
        return dataset

    def _merge_dataframes(
        self,
        source: Union[pd.DataFrame, DataFrame],
        destination: Union[pd.DataFrame, DataFrame],
        result_cols: list,
    ) -> Union[pd.DataFrame, DataFrame]:
        """Merges two DataFrames of the same type.

        Args:
            source (Union[pd.DataFrame, DataFrame]): The source dataframe.
            destination (Union[pd.DataFrame, DataFrame]): The destination dataframe.
            result_cols (list): The columns from the destination dataframe to include
                in the merge.

        Returns:
            Union[pd.DataFrame, DataFrame]: The merged dataframe.

        Raises:
            TypeError: If the source and destination datasets have incompatible types.
        """
        if isinstance(source, pd.DataFrame) and isinstance(destination, pd.DataFrame):
            return self._merge_pandas_df(
                source=source,
                destination=destination,
                result_cols=result_cols,
            )
        elif isinstance(source, DataFrame) and isinstance(destination, DataFrame):
            return self._merge_spark_df(
                source=source,
                destination=destination,
                result_cols=result_cols,
            )
        else:
            msg = f"Source and destination datasets have incompatible types: Source: {type(source)}\tDestination: {type(destination)}"
            raise TypeError(msg)

    def _merge_pandas_df(
        self, source: pd.DataFrame, destination: pd.DataFrame, result_cols: list
    ) -> pd.DataFrame:
        """Merges two Pandas dataframes.

        Args:
            source (pd.DataFrame): The source dataframe.
            destination (pd.DataFrame): The destination dataframe.
            result_cols (list): The columns from the destination dataframe to include
                in the merge.

        Returns:
            pd.DataFrame: The merged dataframe.
        """
        source["id"] = source["id"].astype("string")
        destination["id"] = destination["id"].astype("string")
        return source.merge(destination[result_cols], how="left", on="id")

    def _merge_spark_df(
        self, source: DataFrame, destination: DataFrame, result_cols: list
    ) -> DataFrame:
        """Merges two Spark dataframes.

        Args:
            source (DataFrame): The source dataframe.
            destination (DataFrame): The destination dataframe.
            result_cols (list): The columns from the destination dataframe to include
                in the join.

        Returns:
            DataFrame: The merged dataframe.
        """
        destination_subset = destination.select(*result_cols)
        return source.join(destination_subset, on="id", how="left")
