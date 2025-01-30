#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/base/stage.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 03:43:30 am                                              #
# Modified   : Wednesday January 29th 2025 08:08:19 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from typing import List, Optional

import pandas as pd
from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.asset.dataset.dataset import Dataset
from genailab.asset.dataset.identity import DatasetPassport
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.task import Task
from genailab.infra.exception.object import ObjectNotFoundError
from genailab.infra.persist.repo.dataset import DatasetRepo
from genailab.infra.service.logging.stage import stage_logger
from genailab.infra.utils.visual.print import Printer
from git import Union
from pyspark.sql import DataFrame, SparkSession

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

        if self._dataset_exists(config=self._source_config):
            # Check cache if not forcing execution and return if cache is fresh.
            if self._fresh_cache_exists() and not force:
                dataset = self._get_dataset(config=self._target_config)
                msg = f"Obtained the {self.stage.label} target dataset {dataset.asset_id} from cache."
                self._logger.debug(msg)
                msg += "\nTo force execution, run the stage with force=True."
                printer.print_string(string=msg)
                return dataset
            else:
                return self._run()
        else:
            self._stop_spark()
            msg = f"Unable to run {self.__class__.__name__}. The source dataset {self._source_config.name}, from {self._source_config.phase.label}-{self._source_config.stage.label}, does not exist."
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _fresh_cache_exists(self) -> bool:
        """Checks if a fresh cache of the target dataset exists.

        Returns:
            bool: True if a fresh cache exists, False otherwise.
        """
        source_meta = self._get_dataset(config=self._source_config, meta_only=True)

        if not source_meta.consumed:
            msg = (
                f"The source dataset {source_meta.asset_id} has not yet been consumed."
            )
            self._logger.debug(msg)
            return False

        if not self._dataset_exists(config=self._target_config):
            msg = f"The target dataset cache for {self.phase.label}/{self.stage.label} does not exist in the repository."
            self._logger.debug(msg)
            return False

        msg = f"The target dataset cache for {self.phase.label}/{self.stage.label} exists in the repository."
        self._logger.debug(msg)
        return True

    def _run(self) -> Dataset:
        """Executes the stage tasks and saves the resulting dataset.

        Returns:
            Dataset: The processed dataset.
        """
        # Remove existing target dataset if it exists.
        self._remove_dataset(config=self._target_config)

        # Obtain the source dataset from the repo.
        source = self._get_dataset(config=self._source_config)
        dataframe = source.dataframe

        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                self._logger.error(f"Error in task {task.__class__.__name__}: {e}")
                raise RuntimeError(f"Error in task {task.__class__.__name__}: {e}")

        target = self._create_dataset(
            source=source.passport, config=self._target_config, dataframe=dataframe
        )
        target = self._repo.add(dataset=target, entity=self.__class__.__name__)

        source.consume(entity=self.__class__.__name__)
        self._repo.update(dataset=source)

        return target

    def _create_dataset(
        self,
        config: DatasetConfig,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
        source: Optional[DatasetPassport] = None,
    ) -> Dataset:
        """Creates a Dataset object based on configuration and a dataframe.

        Args:
        config (DatasetConfig): Configuration for the dataset.
            source (Optional[DatasetPassport]): An optional source dataset's passport.
            dataframe (Union[pd.DataFrame, DataFrame]): The dataframe content.

        Returns:
            Dataset: The resulting Dataset object.
        """
        return (
            self._dataset_builder.from_config(config=config)
            .creator(creator=self.__class__.__name__)
            .source(source=source)
            .dataframe(dataframe=dataframe)
            .build()
        )

    def _get_dataset(self, config: DatasetConfig, meta_only: bool = False) -> Dataset:
        """Retrieves a dataset from file or the repository.

        Args:
            config (DatasetConfig): Dataset configuration
            meta_only (bool): Whether to return the metadata only. Default is False

        Returns:
            Dataset: The dataset object.

        """
        """Retrieves a dataset from the repository if it exists."""
        asset_id = self._repo.get_asset_id(
            phase=config.phase, stage=config.stage, name=config.name
        )
        try:
            if meta_only:
                return self._repo.get_meta(asset_id=asset_id)
            else:
                dataset = self._repo.get(
                    asset_id=asset_id,
                    dftype=config.dftype,
                    spark=self._spark,
                    entity=self.__class__.__name__,
                )
                # Verify dataframe type is as requested
                self._validate_dataframe_type(config=config,dataframe=dataset.dataframe)
                return dataset
        except ObjectNotFoundError as e:
            msg = f"Dataset {asset_id} not found in the repository.\n{e}"
            self._logger.error(msg)
            raise ObjectNotFoundError(msg)
        except Exception as e:
            msg = f"Unexpected exception occurred while getting dataset {asset_id} from the repository.\n{e}"
            self._logger.exception(msg)
            raise Exception(msg)

    def _dataset_exists(self, config: DatasetConfig) -> bool:
        """Checks existence of a dataset given a configuration."""
        asset_id = self._repo.get_asset_id(
            phase=config.phase,
            stage=config.stage,
            name=config.name,
        )

        return self._repo.exists(asset_id=asset_id)

    def _remove_dataset(self, config: DatasetConfig) -> None:
        """Removes a dataset from the repository if it exists.

        Args:
            phase (PhaseDef): The phase of the dataset.
            stage (StageDef): The stage of the dataset.
            name (str): The name of the dataset.
        """
        asset_id = self._repo.get_asset_id(
            phase=config.phase, stage=config.stage, name=config.name
        )
        if self._repo.exists(asset_id=asset_id):
            self._repo.remove(asset_id=asset_id)

    def _validate_dataframe_type(self, config: DatasetConfig, dataframe: Union[pd.core.frame.DataFrame, pd.DataFrame, DataFrame]) -> None:
        if ((config.dftype == DFType.PANDAS and isinstance(dataframe, DataFrame)) or (config.dftype in(DFType.SPARK, DFType.SPARKNLP) and not isinstance(dataframe, DataFrame))):
            msg = f"DataFrame type returned from the repository is invalid. Expected {config.dftype.value}. Received {type(dataframe)}"
            self._logger.error(msg)
            raise TypeError(msg)