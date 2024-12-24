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
# Modified   : Monday December 23rd 2024 10:05:03 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC
from enum import Enum
from typing import Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.asset.dataset.dataset import Dataset, DatasetMeta
from discover.asset.idgen.dataset import DatasetIDGen
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameStructure, NestedNamespace
from discover.core.flow import PhaseDef, StageDef
from discover.flow.task.base import Task, TaskBuilder
from discover.infra.persist.repo.asset import DatasetRepo
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
        asset_idgen (Type[DatasetIDGen], optional): The class responsible for generating asset IDs.
            Defaults to DatasetIDGen.
        repo (DatasetRepo, optional): The repository interface for dataset operations.
            Defaults to `DiscoverContainer.repo.dataset_repo`.
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
        asset_idgen: Type[DatasetIDGen] = DatasetIDGen,
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
        self._source_asset_id = None
        self._destination_asset_id = None

        # Indicates whether to use pandas, spark, or sparknlp DataFrames
        self._source_dataframe_structure = DataFrameStructure.from_identifier(
            self._source_config.dataframe_structure
        )
        self._destination_dataframe_structure = DataFrameStructure.from_identifier(
            self._destination_config.dataframe_structure
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
        self._source_asset_id = self._get_asset_id(config=self._source_config)
        self._destination_asset_id = self._get_asset_id(config=self._destination_config)

        # Determine execution path
        execution_path = self._get_execution_path()

        # Direct the process
        if execution_path == ExecutionPath.RUN:
            self._logger.debug("Execution path: RUN")
            self._run()

        elif execution_path == ExecutionPath.UPDATE_ENDPOINT:
            self._logger.debug("Execution path: UPDATE ENDPOINT")
            self._update_endpoint()

        else:
            self._logger.debug("Execution path: RETURN")
        return self._destination_asset_id

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
        endpoint_exists = self._dataset_exists(asset_id=self._destination_asset_id)
        source_consumed = self._source_consumed(asset_id=self._source_asset_id)

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
            asset_id=self._source_asset_id,
            dataframe_structure=self._source_dataframe_structure,
        )
        data = source_dataset.content
        for task in self._tasks:
            data = task.run(data=data)

        # Create the new destination dataset including metadata
        destination_dataset = self._create_dataset(
            config=self._destination_config,
            data=data,
        )

        # Mark the source dataset as consumed and persist the metadata.
        source_dataset.meta.consume(asset_id=destination_dataset.meta.asset_id)
        self._save_dataset_metadata(dataset_meta=source_dataset.meta)

        # Persist pipeline stage destination dataset
        self._save_dataset(dataset=destination_dataset, replace_if_exists=True)

    def _update_endpoint(self) -> None:
        """Updates the endpoint dataset with changes in the source dataset."""
        # Load the source and destination datasets
        source_dataset = self._load_dataset(
            asset_id=self._source_asset_id,
            dataframe_structure=self._source_dataframe_structure,
        )
        destination_dataset = self._load_dataset(
            asset_id=self._destination_asset_id,
            dataframe_structure=self._destination_dataframe_structure,
        )
        # Obtain the columns to merge
        merge_cols = [
            col
            for col in destination_dataset.content.columns
            if col.startswith(self.stage.id) or col == "id"
        ]
        # Merge the DataFrames and update the destination dataset
        destination_dataset.content = self._merge_dataframes(
            source=source_dataset.content,
            destination=destination_dataset.content,
            merge_cols=merge_cols,
        )

        # Mark the source dataset as consumed and persist the metadata.
        source_dataset.meta.consume(asset_id=destination_dataset.meta.asset_id)
        self._save_dataset_metadata(dataset_meta=source_dataset.meta)

        # Persist pipeline stage destination dataset
        self._save_dataset(dataset=destination_dataset, replace_if_exists=True)

    def _source_consumed(self, asset_id: str) -> bool:
        """Returns True of the source has been consumed"""
        dataset_meta = self._load_dataset_metadata(asset_id=asset_id)
        return dataset_meta.consumed

    def _merge_dataframes(
        self,
        source: DataFrameStructure,
        destination: DataFrameStructure,
        merge_cols: list,
    ) -> DataFrameStructure:
        """Merges two DataFrames of the same type.

        Args:
            source (DataFrameStructure): The source dataframe.
            destination (DataFrameStructure): The destination dataframe.
            merge_cols (list): The columns from the destination dataframe to include
                in the merge.

        Returns:
            DataFrameStructure: The merged dataframe.

        Raises:
            TypeError: If the source and destination datasets have incompatible types.
        """
        if isinstance(source, pd.DataFrame) and isinstance(destination, pd.DataFrame):
            return self._merge_pandas_df(
                source=source,
                destination=destination,
                merge_cols=merge_cols,
            )
        elif isinstance(source, DataFrame) and isinstance(destination, DataFrame):
            return self._merge_spark_df(
                source=source,
                destination=destination,
                merge_cols=merge_cols,
            )
        else:
            msg = f"Source and destination datasets have incompatible types: Source: {type(source)}\tDestination: {type(destination)}"
            raise TypeError(msg)

    def _merge_pandas_df(
        self, source: pd.DataFrame, destination: pd.DataFrame, merge_cols: list
    ) -> pd.DataFrame:
        """Merges two Pandas dataframes.

        Args:
            source (pd.DataFrame): The source dataframe.
            destination (pd.DataFrame): The destination dataframe.
            merge_cols (list): The columns from the destination dataframe to include
                in the merge.

        Returns:
            pd.DataFrame: The merged dataframe.
        """
        source["id"] = source["id"].astype("string")
        destination["id"] = destination["id"].astype("string")
        return source.merge(destination[merge_cols], how="left", on="id")

    def _merge_spark_df(
        self, source: DataFrame, destination: DataFrame, merge_cols: list
    ) -> DataFrame:
        """Merges two Spark dataframes.

        Args:
            source (DataFrame): The source dataframe.
            destination (DataFrame): The destination dataframe.
            merge_cols (list): The columns from the destination dataframe to include
                in the join.

        Returns:
            DataFrame: The merged dataframe.
        """
        destination_subset = destination.select(*merge_cols)
        return source.join(destination_subset, on="id", how="left")

    def _create_dataset(
        self,
        config: NestedNamespace,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
    ) -> Dataset:
        """Creates a dataset.

        Args:
            config (NestedNamespace): Configuration for the dataset.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The dataset payload.

        Returns:
            Dataset: The created dataset.
        """

        meta = DatasetMeta(
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
        )
        return Dataset(meta=meta, content=data)

    @classmethod
    def build(
        cls,
        stage_config: dict,
        return_dataframe_structure: str = "pandas",
        force: bool = False,
        **kwargs,
    ) -> Stage:
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
        dataframe_structure: DataFrameStructure = DataFrameStructure.PANDAS,
    ) -> Dataset:
        """Loads a dataset from the repository.

        Args:
            asset_id (str): The identifier for the dataset asset.
            dataframe_structure (DataFrameStructure): Configuration for a pandas, spark, or sparknlp DataFrame.

        Returns:
            Dataset object

        Raises:
            FileNotFoundError: If the source dataset cannot be found or loaded.
            RuntimeError: If an unrecognized error occurs during loading.
        """
        try:
            dataset = self._repo.get(
                asset_id=asset_id,
                dataframe_structure=dataframe_structure,
            )

            if (
                dataframe_structure.distributed
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

    def _load_dataset_metadata(self, asset_id: str) -> DatasetMeta:
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
            if self._repo.exists(asset_id=dataset.meta.asset_id) and replace_if_exists:
                self._repo.remove(asset_id=dataset.meta.asset_id)
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset\n{dataset} {str(e)}")

    def _save_dataset_metadata(self, dataset_meta: DatasetMeta) -> None:
        """Saves dataset metadata to the repository.

        Args:
            dataset_meta (DatasetMeta): The dataset metadata object.

        Raises:
            RuntimeError: If saving the metadata fails.
        """
        try:
            self._repo.update_dataset_metadata(dataset_meta=dataset_meta)
        except Exception as e:
            raise RuntimeError(
                f"Failed to save dataset metadata\n{dataset_meta}\n{str(e)}"
            )

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
