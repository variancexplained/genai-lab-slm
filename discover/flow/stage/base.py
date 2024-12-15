#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/base.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Sunday December 15th 2024 06:21:21 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC
from datetime import datetime
from enum import Enum
from typing import Optional, Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameType, NestedNamespace
from discover.core.flow import PhaseDef, StageDef
from discover.flow.task.base import Task, TaskBuilder
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                     EXECUTION PATH                                               #
# ------------------------------------------------------------------------------------------------ #
class ExecutionPath(Enum):
    RUN = "run"
    UPDATE_ENDPOINT = "update_endpoint"
    GET_RETURN_VALUE = "get_return_value"


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class representing a stage in a data pipeline.

    This class defines the structure and functionality for a pipeline stage. Each stage
    contains a series of tasks, operates on source and destination configurations, and
    interfaces with a dataset repository. The class is designed to be extended to implement
    specific stages of a data pipeline.

    Args:
        phase (PhaseDef): The phase to which the stage belongs.
        stage (StageDef): The specific stage identifier within the pipeline.
        source_config (dict): The configuration for the source data.
        destination_config (dict): The configuration for the destination data.
        asset_idgen (Type[AssetIDGen], optional): The class responsible for generating asset IDs.
            Defaults to AssetIDGen.
        repo (DatasetRepo, optional): The repository interface for dataset operations.
            Defaults to `DiscoverContainer.repo.dataset_repo`.
        return_dataset (bool, optional): Indicates whether the dataset should be returned after processing.
            Defaults to False.
        force (bool, optional): If true, forces execution even if the destination dataset exists.
            Defaults to False.
        **kwargs: Additional keyword arguments for stage customization.
    """

    _task_builder = TaskBuilder

    @inject
    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        return_dataset: bool = False,
        force: bool = False,
        **kwargs,
    ) -> None:
        self._phase = phase
        self._stage = stage
        self._source_config = NestedNamespace(source_config)
        self._destination_config = NestedNamespace(destination_config)
        self._asset_idgen = asset_idgen
        self._repo = repo
        self._return_dataset = return_dataset
        self._force = force
        self._tasks = []
        # Dataset asset identifiers for use in subclasses.
        self.source_asset_id = None
        self.destination_asset_id = None

        # Indicates whether to use pandas, spark, or sparknlp DataFrames
        self.source_dataframe_type = DataFrameType.from_identifier(
            self._source_config.dataframe_type
        )
        self.destination_dataframe_type = DataFrameType.from_identifier(
            self._destination_config.dataframe_type
        )
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self._phase

    @property
    def stage(self) -> StageDef:
        """Returns the specific stage within the pipeline."""
        return self._stage

    def add_task(self, task: Task) -> None:
        """Adds a task to the stage and assigns the stage identifier to the task.

        Args:
            task (DataEnhancerTask): The task to be added to the stage.

        Raises:
            TypeError: If the task is not an instance of the expected Task class.
        """
        if not isinstance(task, Task):
            raise TypeError(
                f"Expected an instance or subclass of Task. Received type {type(task)}"
            )
        self._tasks.append(task)

    @stage_logger
    def run(self) -> Dataset:
        """
        Executes the stage and directs the flow based on the execution path.

        This method is responsible for determining the execution path (RUN, UPDATE_ENDPOINT, or RETURN)
        and delegating to the appropriate logic to process the dataset. The method is decorated with
        the stage_logger to log the execution flow.

        Returns:
            Dataset: The resulting dataset after the appropriate processing based on the execution path.
        """
        return self._core_stage_run()

    def _core_stage_run(self) -> Dataset:
        """
        Core logic for running the stage, determining the execution path,
        and delegating to the appropriate processing function.

        This method performs the following steps:
        1. Retrieves the source and destination asset IDs using the provided configurations.
        2. Determines the execution path based on the current asset IDs.
        3. Executes the appropriate processing logic depending on the execution path:
        - RUN: Executes the main processing logic.
        - UPDATE_ENDPOINT: Updates the endpoint.
        - RETURN: Returns the dataset without modification.

        Returns:
            Dataset: The resulting dataset after the execution path has been followed.
        """
        self.source_asset_id = self._get_asset_id(config=self._source_config)
        self.destination_asset_id = self._get_asset_id(config=self._destination_config)

        # Determine execution path
        execution_path = self._get_execution_path()

        # Direct the process
        if execution_path == ExecutionPath.RUN:
            self._logger.debug("Execution path: RUN")
            dataset = self._run()
            return self._get_return_value(dataset=dataset)

        elif execution_path == ExecutionPath.UPDATE_ENDPOINT:
            self._logger.debug("Execution path: UPDATE ENDPOINT")
            dataset = self._update_endpoint()
            return self._get_return_value(dataset=dataset)

        else:
            self._logger.debug("Execution path: RETURN")
            return self._get_return_value(
                dataframe_type=self.destination_dataframe_type
            )

    def _get_execution_path(self) -> ExecutionPath:
        """Determines the execution path

        There are thee binary variables creating 2^8 possible conditions that resolve to
        one of three actions:
            1. Run The pipeline and return
            2. Update the existing endpoint with new source data and return
            3. Get the return value requested. No action required.

        Option 1 executes if Force is true or the endpoint doesn't exist.
        Option 2 executes if not forcing, the endpoint exists, but there is new source data.
        Option 3 executes if not forcing, the endpoint exists and the source  has
            been consumed.

        Note: Force must be set to True if the source code has changed or the output changes with
        the input, and the input has changed.
        """
        # Obtain current state
        endpoint_exists = self._dataset_exists(asset_id=self.destination_asset_id)
        source_consumed = self._source_consumed(asset_id=self.source_asset_id)

        # Easy case first
        if not self._force and source_consumed and endpoint_exists:
            return ExecutionPath.GET_RETURN_VALUE
        # Update endpoint if the soure has changed
        elif not self._force and not source_consumed and endpoint_exists:
            return ExecutionPath.UPDATE_ENDPOINT
        # Otherwise run
        else:
            return ExecutionPath.RUN

    def _run(self) -> Dataset:
        """Performs the core logic of the stage, executing tasks in sequence."""
        source_dataset = self._load_dataset(
            asset_id=self.source_asset_id, dataframe_type=self.source_dataframe_type
        )
        data = source_dataset.content
        for task in self._tasks:
            data = task.run(data=data)

        # Create the new destination dataset
        destination_dataset = self._create_dataset(
            asset_id=self.destination_asset_id,
            config=self._destination_config,
            dataframe_type=self.destination_dataframe_type,
            data=data,
        )

        self._save_dataset(dataset=destination_dataset, replace_if_exists=True)
        # Mark the source dataset as having been consumed.
        self._mark_source_consumed(asset_id=self.source_asset_id)

        return destination_dataset

    def _update_endpoint(self) -> Dataset:
        """Updates the endpoint dataset with changes in the source dataset.

        Returns:
            Dataset: The updated dataset endpoint
        """
        source_dataset = self._load_dataset(
            asset_id=self.source_asset_id, dataframe_type=self.source_dataframe_type
        )
        destination_dataset = self._load_dataset(
            asset_id=self.destination_asset_id,
            dataframe_type=self.destination_dataframe_type,
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
            self.destination_asset_id,
            config=self._destination_config,
            dataframe_type=self.destination_dataframe_type,
            data=df,
        )
        # Save the updated dataset
        self._save_dataset(dataset=destination_dataset_updated, replace_if_exists=True)
        # Mark the source dataset as having been consumed.
        self._mark_source_consumed(asset_id=source_dataset.asset_id)
        return destination_dataset_updated

    def _source_consumed(self, asset_id: str) -> bool:
        """Returns True of the source has been consumed"""
        dataset_meta = self._load_dataset_metadata(asset_id=asset_id)
        return dataset_meta.consumed

    def _mark_source_consumed(self, asset_id: str) -> None:
        """Marks the dataset as having been consumed.

        Args:
            dataset (Dataset): The dataset to mark as consumed.

        """
        dataset = self._load_dataset_metadata(asset_id=asset_id)
        dataset.consumed = True
        dataset.dt_consumed = datetime.now()
        dataset.consumed_by = self.__class__.__name__
        self._save_dataset_metadata(dataset=dataset)

    def _merge_dataframes(
        self,
        source: DataFrameType,
        destination: DataFrameType,
        result_cols: list,
    ) -> DataFrameType:
        """Merges two DataFrames of the same type.

        Args:
            source (DataFrameType): The source dataframe.
            destination (DataFrameType): The destination dataframe.
            result_cols (list): The columns from the destination dataframe to include
                in the merge.

        Returns:
            DataFrameType: The merged dataframe.

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

    def _create_dataset(
        self,
        asset_id: str,
        config: NestedNamespace,
        dataframe_type: DataFrameType,
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
            dataframe_type=dataframe_type,
        )

    @classmethod
    def build(cls, stage_config: dict, force: bool = False, **kwargs) -> Stage:
        """Creates and returns a new stage instance from the provided configuration.

        Args:
            stage_config (dict): The configuration dictionary containing source, destination, and tasks details.
            force (bool, optional): Whether to force execution even if the destination dataset exists. Defaults to False.

        Returns:
            Stage: A new instance of the Stage class, configured with the provided settings.

        Raises:
            KeyError: If the required keys are missing from the stage_config.
            ValueError: If tasks cannot be built from the configuration.
            RuntimeError: If there is an error creating the stage.
        """

        try:
            # Instantiate the Stage object
            stage = cls(
                phase=PhaseDef.from_value(stage_config["phase"]),
                stage=StageDef.from_value(stage_config["stage"]),
                source_config=stage_config["source_config"],
                destination_config=stage_config["destination_config"],
                force=force,
                **kwargs,
            )

            # Construct the Stage's Task objects
            tasks = [
                cls._task_builder.build(
                    phase=PhaseDef.from_value(stage_config["phase"]),
                    stage=StageDef.from_value(stage_config["stage"]),
                    task_config=task_config,
                )
                for task_config in stage_config["tasks"]
            ]

            # Add Tasks to the Stage object
            for task in tasks:
                stage.add_task(task)

            return stage

        except KeyError as e:
            raise ValueError(
                f"Missing required configuration key in stage_config: {str(e)}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to build stage from configuration: {str(e)}"
            ) from e

    def _get_asset_id(self, config: NestedNamespace) -> str:
        """Returns the asset id given an asset configuration.

        Args:
            config (NestedNamespace): An asset configuration.

        Returns:
            str: The asset_id for the asset.
        """
        return self._asset_idgen.get_asset_id(
            asset_type=config.asset_type,
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
        )

    def _load_file(self, filepath: str) -> pd.DataFrame:
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

    def _load_dataset(
        self,
        asset_id: str,
        dataframe_type: DataFrameType,
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """Loads a dataset from the repository.

        Args:
            asset_id (str): The identifier for the dataset asset.
            dataframe_type (DataFrameType): Configuration for a pandas, spark, or sparknlp DataFrame.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: Pandas or PySpark DataFrame.

        Raises:
            FileNotFoundError: If the source dataset cannot be found or loaded.
            RuntimeError: If an unrecognized error occurs during loading.
        """
        try:
            dataset = self._repo.get(
                asset_id=asset_id,
                dataframe_type=dataframe_type,
            )

            if (
                dataframe_type.distributed
                and "__index_level_0__" in dataset.content.columns
            ):
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
        dataframe_type: Optional[DataFrameType] = None,
        return_dataset: bool = False,
    ) -> Union[Dataset, str]:
        """Obtains and returns a value based on the boolean `return_dataset` instance variable."""
        return_dataset = return_dataset or self._return_dataset
        dataframe_type = dataframe_type or DataFrameType.from_identifier("pandas")

        if return_dataset:
            try:
                return dataset or self._load_dataset(
                    asset_id=asset_id, dataframe_type=dataframe_type
                )
            except Exception as e:
                msg = f"Unable to return dataset. The dataset or a valid asset_id and configuration must be provided.\n{e}"
                raise ValueError(msg)
        else:
            try:
                return asset_id or dataset.asset_id
            except Exception as e:
                msg = f"Unable to return an asset_id. Either the  asset_id or a dataset with an asset_id must be provided.\n{e}"
                raise ValueError(msg)
