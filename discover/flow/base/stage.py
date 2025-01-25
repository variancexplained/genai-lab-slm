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
# Modified   : Friday January 24th 2025 10:38:46 pm                                                #
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
from discover.asset.dataset.config import DatasetConfig, FilesetConfig
from discover.asset.dataset.dataset import Dataset
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.task import Task
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """Abstract base class for defining a stage in a data pipeline.

    This class provides a framework for implementing a data pipeline stage, including
    configuration, task execution, and dataset management. Subclasses must implement
    the `phase`, `stage`, and `dftype` properties, as well as any stage-specific logic.

    Args:
        source_config (DatasetConfig): Configuration for the source dataset.
        target_config (DatasetConfig): Configuration for the target dataset.
        tasks (List[Task]): List of tasks to be executed within this stage.
        repo (DatasetRepo): Repository for dataset storage and management.
        dataset_builder (DatasetBuilder): Builder for creating `Dataset` objects.
        spark (Optional[SparkSession]): Optional Spark session for distributed processing.

    Attributes:
        _source_config (DatasetConfig): Stores the configuration for the source dataset.
        _target_config (DatasetConfig): Stores the configuration for the target dataset.
        _tasks (List[Task]): List of tasks to execute in the stage.
        _repo (DatasetRepo): Repository for managing datasets.
        _dataset_builder (DatasetBuilder): Builder for constructing datasets.
        _spark (Optional[SparkSession]): Optional Spark session for distributed data processing.
        _source (Optional[Dataset]): Reference to the source dataset.
        _target (Optional[Dataset]): Reference to the target dataset.
        _logger (Logger): Logger instance for the stage.
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

        Returns:
            PhaseDef: The phase definition of the stage.
        """
        pass

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        """Abstract property to define the stage identifier.

        Returns:
            StageDef: The stage definition.
        """
        pass

    @property
    @abstractmethod
    def dftype(self) -> DFType:
        """Abstract property to define the dataframe type.

        Returns:
            DFType: The dataframe type used in the stage.
        """
        pass

    @stage_logger
    def run(self, force: bool = False) -> Dataset:
        """Executes the stage, optionally forcing re-execution.

        Args:
            force (bool): If True, forces re-execution of the stage. Defaults to False.
            test_mode (bool): Development accommodation. Indicates whether the
                stage is being run in test mode.

        Returns:
            Dataset: The resulting dataset after stage execution.
        """

        if self._source_exists(config=self._source_config):
            # Check cache if not forcing execution and return if cache is fresh.
            if self._fresh_cache_exists() and not force:
                dataset = self._get_dataset(
                    phase=self._target_config.phase,
                    stage=self._target_config.stage,
                    name=self._target_config.name,
                )
                msg = f"Obtained the {self.stage.label} target dataset {dataset.asset_id} from cache."
                self._logger.debug(msg)
                msg += "\nTo force execution, run the stage with force=True."
                printer.print_string(string=msg)
                return dataset
            else:
                return self._run()
        else:
            msg = f"Unable to run {self.__class__.__name__}. The source dataset {self._source_config.name}, from {self._source_config.phase.label}-{self._source_config.stage.label}, does not exist."
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _fresh_cache_exists(self) -> bool:
        """Checks if a fresh cache of the target dataset exists.

        Returns:
            bool: True if a fresh cache exists, False otherwise.
        """
        source_asset_id = self._repo.get_asset_id(
            phase=self._source_config.phase,
            stage=self._source_config.stage,
            name=self._source_config.name,
        )
        source_meta = self._repo.get_meta(asset_id=source_asset_id)

        if not source_meta.consumed:
            msg = f"The source dataset {source_asset_id} has not  yet been consumed."
            self._logger.debug(msg)
            return False

        target_asset_id = self._repo.get_asset_id(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        if not self._repo.exists(asset_id=target_asset_id):
            msg = f"The target dataset {target_asset_id} cache does not exist in the repository."
            self._logger.debug(msg)
            return False

        msg = f"A fresh cache for target dataset {target_asset_id} exists."
        self._logger.debug(msg)
        return True

    def _run(self) -> Dataset:
        """Executes the stage tasks and saves the resulting dataset.

        Returns:
            Dataset: The processed dataset.
        """
        self._remove_dataset(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )

        source = self._get_dataset(
            phase=self._source_config.phase,
            stage=self._source_config.stage,
            name=self._source_config.name,
        )
        dataframe = source.dataframe

        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                self._logger.error(f"Error in task {task.__class__.__name__}: {e}")
                raise RuntimeError(f"Error in task {task.__class__.__name__}: {e}")

        target = self._create_dataset(
            source=source, config=self._target_config, dataframe=dataframe
        )
        target = self._repo.add(dataset=target, entity=self.__class__.__name__)

        source.consume(entity=self.__class__.__name__)
        self._repo.update(dataset=source)

        return target

    def _create_dataset(
        self,
        source: Dataset,
        config: DatasetConfig,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
    ) -> Dataset:
        """Creates a Dataset object based on configuration and a dataframe.

        Args:
            source (Dataset): The source dataset.
            config (DatasetConfig): Configuration for the dataset.
            dataframe (Union[pd.DataFrame, DataFrame]): The dataframe content.

        Returns:
            Dataset: The resulting Dataset object.
        """
        return (
            self._dataset_builder.from_config(config=config)
            .creator(creator=self.__class__.__name__)
            .source(source=source.passport)
            .dataframe(dataframe=dataframe)
            .build()
        )

    def _get_dataset(self, phase: PhaseDef, stage: StageDef, name: str) -> Dataset:
        """Retrieves a dataset from the repository.

        Args:
            phase (PhaseDef): The phase of the dataset.
            stage (StageDef): The stage of the dataset.
            name (str): The name of the dataset.

        Returns:
            Dataset: The dataset object.

        Raises:
            ObjectNotFoundError: If the dataset is not found.
            TypeError: If the retrieved object is not a Dataset.
        """
        asset_id = self._repo.get_asset_id(phase=phase, stage=stage, name=name)
        dataset = self._repo.get(
            asset_id=asset_id,
            dftype=self.dftype,
            spark=self._spark,
            entity=self.__class__.__name__,
        )
        if not isinstance(dataset, Dataset):
            self._logger.error(
                f"Expected a Dataset object for {asset_id}. Received {type(dataset)}."
            )
            raise TypeError(f"Invalid dataset type for {asset_id}.")
        return dataset

    def _source_exists(self, config: Union[FilesetConfig, DatasetConfig]) -> bool:
        """Checks existence of a dataset, given its configuration."""
        asset_id = self._repo.get_asset_id(
            phase=config.phase,
            stage=config.stage,
            name=config.name,
        )

        return self._repo.exists(asset_id=asset_id)

    def _remove_dataset(self, phase: PhaseDef, stage: StageDef, name: str) -> None:
        """Removes a dataset from the repository if it exists.

        Args:
            phase (PhaseDef): The phase of the dataset.
            stage (StageDef): The stage of the dataset.
            name (str): The name of the dataset.
        """
        asset_id = self._repo.get_asset_id(phase=phase, stage=stage, name=name)
        if self._repo.exists(asset_id=asset_id):
            self._repo.remove(asset_id=asset_id)
