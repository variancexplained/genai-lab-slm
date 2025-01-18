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
# Modified   : Friday January 17th 2025 10:39:14 pm                                                #
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

from discover.asset.dataset.builder import DatasetBuilder, DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetConfig
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.task import Task
from discover.infra.exception.object import ObjectNotFoundError
from discover.infra.persist.object.flowstate import FlowState
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
        state: FlowState,
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
        self._state = state
        self._repo = repo
        self._dataset_builder = dataset_builder
        self._spark = spark

        self._source = None
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
        # If the target exists and we're not forcing execution, return the target
        if (
            self._state.exists(
                phase=self._target_config.phase,
                stage=self._target_config.stage,
                name=self._target_config.name,
            )
            and not force
        ):
            return self._get_target()
        # If the state exists, and we're forcing remove the target
        elif self._state.exists(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        ):
            self._remove_target()
        # Run the stage pipeline.
        return self._run()

    def _run(self) -> Dataset:
        """Internal method to execute tasks and save the resulting dataset.

        This method handles the retrieval of source data, execution of the configured
        tasks in sequence, and saving of the processed dataset.

        Returns:
            Dataset: The processed dataset.
        """
        source = self.get_source_dataset()
        dataframe = source.dataframe
        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        self._target = self.save_target_dataset(source=source, dataframe=dataframe)
        return self._target

    def get_source_dataset(self) -> Dataset:
        """Retrieves the source dataset based on the configuration.

        This method extracts the phase and stage from the source configuration,
        reads the corresponding dataset passport from the flow state, and retrieves
        the dataset from the repository.

        Returns:
            Dataset: The source dataset.

        Example:
            >>> source_dataset = stage.get_source_dataset()
        """
        passport = self._state.read(
            phase=self._source_config.phase,
            stage=self._source_config.stage,
            name=self._source_config.name,
        )
        return self._repo.get(
            asset_id=passport.asset_id, spark=self._spark, dftype=self.dftype
        )

    def save_target_dataset(
        self, source: Dataset, dataframe: Union[pd.DataFrame, DataFrame]
    ) -> Dataset:
        """Saves the target dataset after processing.

        Constructs the dataset passport, builds the dataset, and adds it to the repository.
        This method ensures the processed data is stored and can be retrieved in subsequent stages.

        Args:
            source (Dataset): The source dataset.
            dataframe (Union[pd.DataFrame, DataFrame]): The processed dataframe.

        Returns:
            Dataset: The saved target dataset.

        Example:
            >>> target_dataset = stage.save_target_dataset(source, dataframe)
        """
        passport = (
            DatasetPassportBuilder()
            .phase(self._target_config.phase)
            .stage(self._target_config.stage)
            .source(source.passport)
            .creator(self.__class__.__name__)
            .name(self._target_config.name)
            .build()
            .passport
        )
        target = (
            self._dataset_builder.from_dataframe(dataframe)
            .passport(passport)
            .to_parquet()
            .build()
            .dataset
        )
        self._repo.add(asset=target, dftype=self.dftype)
        self._state.create(passport=target.passport)
        return target

    def _get_target(self) -> Dataset:
        """Retrieves the target dataset from the state and repository.

        Reads the dataset passport from the flow state and retrieves the dataset
        from the repository. Handles exceptions related to data integrity and
        unexpected errors.

        Returns:
            Dataset: The target dataset.

        Raises:
            ObjectNotFoundError: If the dataset is not found in the repository despite
                being registered in the state.
            Exception: For other unknown exceptions during retrieval.

        Example:
            >>> target_dataset = stage._get_target()
        """
        passport = self._state.read(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        try:
            return self._repo.get(
                asset_id=passport.asset_id, dftype=self.dftype, spark=self._spark
            )
        except ObjectNotFoundError as e:
            msg = f"Data Integrity Error encountered while reading target dataset {self._target_config}. Flow state exists, but dataset does not exist in the repository.\n{e}"
            self._logger.error(msg)
            raise
        except Exception as e:
            msg = f"Unknown exception occurred while reading target dataset {self._target_config}.\n{e}"
            self._logger.exception(msg)
            raise

    def _remove_target(self) -> None:
        """Removes a stage target from flow state and the dataset repository"""
        # Obtain the passport containing the asset id for the target
        passport = self._state.read(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        # Remove the dataset from the repository if it exists
        try:
            self._repo.remove(asset_id=passport.asset_id)
        except (ObjectNotFoundError, FileNotFoundError) as e:
            msg = f"Dataset and/or files not found for asset {passport.asset_id}.\n{e}"
            self._logger.warning(msg)
        except Exception as e:
            msg = f"Unknown exception occurred while removing dataset {passport.asset_id} from the repository.\n{e}"
            self._logger.exception(msg)
            raise RuntimeError(msg)
        # Delete the passport from flow state
        self._state.delete(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
