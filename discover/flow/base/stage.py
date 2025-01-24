#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/stage.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 03:43:30 am                                              #
# Modified   : Thursday January 23rd 2025 05:17:35 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd
from git import Union
from pyspark.sql import DataFrame, SparkSession

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.config import DatasetConfig
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.state import DatasetState
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.task import Task
from discover.infra.exception.object import ObjectNotFoundError
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class representing a stage in a data processing flow.

    This class defines the structure and behavior for stages, including handling
    source data, executing tasks, and saving target datasets. Subclasses should
    implement specific details for the phase, stage, and dataframe type.

    Args:
        source_config (Dict[str, str]): Configuration details for the source dataset.
        target_config ((Dict[str, str]): Configuration details for the target dataset.
        tasks (List[Task]): List of tasks to be executed sequentially.
        state (FlowState): State management for the flow.
        repo (DatasetRepo): Repository for dataset storage and retrieval.
        dataset_builder (DatasetBuilder): Builder for creating and saving datasets.
        spark (Optional[SparkSession]): Optional Spark session for data processing.

    Attributes:
        _source_config (Dict[str, str]): Configuration details for the source dataset.
        _target_config (Dict[str, str]): Configuration details for the target dataset.
        _tasks (List[Task]): List of tasks to be executed sequentially.
        _state (FlowState): State management for the flow.
        _repo (DatasetRepo): Repository for dataset storage and retrieval.
        _dataset_builder (DatasetBuilder): Builder for creating and saving datasets.
        _spark (Optional[SparkSession]): Optional Spark session for data processing.
        _source (Optional[Dataset]): The source dataset used in the stage.
        _logger (Logger): Logger instance for the stage.

    Methods:
        phase: Abstract property to define the phase of the stage.
        stage: Abstract property to define the stage identifier.
        dftype: Abstract property to define the dataframe type.
        run(force: bool): Executes the stage, optionally forcing re-execution.
        _run(): Internal method to run the stage tasks and save results.
        get_source_dataset(): Retrieves the source dataset based on the configuration.
        save_target_dataset(source, dataframe): Saves the target dataset after processing.
        _get_target(): Retrieves the target dataset from the state and repository.
    """

    def __init__(
        self,
        source_config: DatasetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        spark: Optional[SparkSession] = None,
    ) -> None:
        """Initializes a Stage instance with the given configuration and components.

        Args:
            source_config (Dict[str, str]): Configuration for the source data.
            target_config ((Dict[str, str]): Configuration details for the target dataset.
            tasks (List[Task]): List of tasks to execute in the stage.
            state (FlowState): Flow state management object.
            repo (DatasetRepo): Dataset repository for storage and retrieval.
            dataset_builder (DatasetBuilder): Builder for creating datasets.
            spark (Optional[SparkSession]): Optional Spark session for processing.
        """
        self._source_config = source_config
        self._target_config = target_config
        self._tasks = tasks
        self._repo = repo
        self._dataset_builder = dataset_builder
        self._spark = spark

        self._source: Optional[Dataset] = None
        self._target: Optional[Dataset] = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    @abstractmethod
    def phase(self) -> PhaseDef:
        """Abstract property to define the phase of the stage.

        Subclasses should implement this to return the phase of the stage,
        which typically correlates with the step in the overall data pipeline.

        Returns:
            PhaseDef: The phase definition of the stage.
        """
        pass

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        """Abstract property to define the stage identifier.

        This should be implemented by subclasses to specify the unique
        identifier for the stage, used in tracking and state management.

        Returns:
            StageDef: The stage definition.
        """
        pass

    @property
    @abstractmethod
    def dftype(self) -> DFType:
        """Abstract property to define the dataframe type.

        Defines the expected type of the dataframe, such as pandas or Spark,
        which guides how data is processed within the stage.

        Returns:
            DFType: The dataframe type used in the stage.
        """
        pass

    @stage_logger
    def run(self, force: bool = False) -> Dataset:
        """Executes the stage, optionally forcing re-execution.

        This method checks if the stage target already exists. If `force`
        is False and the target exists, it retrieves the dataset from the state.
        Otherwise, it executes the stage's tasks and saves the result.

        Args:
            force (bool): If True, forces re-execution of the stage. Defaults to False.

        Returns:
            Dataset: The resulting dataset after stage execution.
        """
        # Determine whether to obtain the target from cache or run the pipeline.
        if self._fresh_cache_exists() and not force:
            return self._get_target()
        else:
            return self._run()

    def _fresh_cache_exists(self) -> bool:
        """Returns a boolean indicating whether a fresh cache of the target is extant.

        A fresh cache is the condition in which the source has been consumed,
        the target exists, implying that the target reflects the source
        and the transformations executed in this stage.

        Returns:
            bool: True if the above condition is met, False otherwise.

        """
        # Get the source asset id
        source_asset_id = self._repo.get_asset_id(
            phase=self._source_config.phase,
            stage=self._source_config.stage,
            name=self._source_config.name,
        )

        # Get the metadata for the source dataset
        source_meta = self._repo.get_meta(asset_id=source_asset_id)

        # If the source has not been consumed, return False
        if not source_meta.status == DatasetState.CONSUMED:
            return False

        # If the target dataset does not exist, return False
        target_asset_id = self._repo.get_asset_id(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        if not self._repo.exists(asset_id=target_asset_id):
            return False
        else:
            return True

    def _run(self) -> Dataset:
        """Internal method to execute tasks and save the resulting dataset.

        This method handles the retrieval of source data, execution of the configured
        tasks in sequence, and saving of the processed dataset.

        Returns:
            Dataset: The processed dataset.
        """
        # Get the source dataset
        source = self._get_dataset(
            phase=self._source_config.phase,
            stage=self._source_config.stage,
            name=self._source_config.name,
        )
        # Extract the underlying dataframe
        dataframe = source.dataframe
        # Process
        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        # Create target dataset and publish to repository
        target = self._create_dataset(
            source=source, config=self._target_config, dataframe=dataframe
        )
        target = self._repo.add(dataset=target)

        # Mark the source dataset as consumed and persist
        source.consume()
        self._repo.update(dataset=source)

        return target

    def _create_dataset(
        self,
        source: Dataset,
        config: DatasetConfig,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
    ) -> Dataset:
        """Creates a Dataset object based on a configuration and given dataframe.

        Args:
            source (Dataset): The source dataset.
            config (DatasetConfig): Configuration for the dataset
            dataframe (Union[pd.DataFrame, DataFrame]): The dataframe content

        Returns:
            Dataset: The Datset object
        """
        return (
            self._dataset_builder.from_config(config=config)
            .creator(creator=self.__class__.__name__)
            .source(source=source.passport)
            .dataframe(dataframe=dataframe)
            .build()
        )

    def _get_dataset(self, phase: PhaseDef, stage: StageDef, name: str) -> Dataset:
        """Retrieves the target dataset from the state and repository.

        Args:
            phase (PhaseDef): The phase for the Dataset.
            stage (StageDef): The stage for the Dataset.
            name (str): The name of the Dataset

        Returns:
            Dataset: The target dataset.

        Raises:
            ObjectNotFoundError: If the dataset is not found.

        Example:
            >>> target_dataset = stage._get_target()
        """
        # Get the asset id
        asset_id = self._repo.get_asset_id(
            phase=phase,
            stage=stage,
            name=name,
        )
        dataset = self._repo.get(asset_id=asset_id)
        if not isinstance(dataset, Dataset):
            msg = f"Expected a Dataset object for dataset {asset_id}. Received a {type(dataset)} object."
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)

        return dataset
