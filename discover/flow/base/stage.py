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
# Modified   : Saturday January 4th 2025 11:48:58 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import pandas as pd
from git import Union
from pyspark.sql import DataFrame, SparkSession

from discover.archive.flow.task.base import Task
from discover.asset.dataset.builder import DatasetBuilder, DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):

    def __init__(
        self,
        source_config: Dict[str, str],
        tasks: List[Task],
        state: FlowState,
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        spark: Optional[SparkSession] = None,
    ) -> None:
        self._source_config = source_config
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
        pass

    @property
    @abstractmethod
    def stage(self) -> StageDef:
        pass

    @property
    @abstractmethod
    def dftype(self) -> DFType:
        pass

    @stage_logger
    def run(self) -> Dataset:
        # Step 1: Get source data
        source = self.get_source_dataset()

        # Step 2: Execute all tasks sequentially
        dataframe = source.dataframe
        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        # Step 3: Save target data
        self._target = self.save_target_dataset(source=source, dataframe=dataframe)

        return self._target

    def get_source_dataset(self) -> Dataset:
        """Obtrains the source data from the state and repo

        Args:
            dftype (Optional[DFType]): The dataframe type to return. If not provided,
                it will return the dataframe type designated in the dataset object.
                This allows datasets saved as pandas dataframes to
                be read using another spark or another dataframe type.
        """
        # Extracts the phase and stage from the source configuration
        source_phase = PhaseDef.from_value(self._source_config["phase"])
        source_stage = StageDef.from_value(self._source_config["stage"])
        # Reads the source passport from the flow state.
        passport = self._state.read(phase=source_phase, stage=source_stage)
        # Obtains the source dataset from the repository.
        return self._repo.get(
            asset_id=passport.asset_id, spark=self._spark, dftype=self.dftype
        )

    def save_target_dataset(
        self, source: Dataset, dataframe: Union[pd.DataFrame, DataFrame]
    ) -> Dataset:
        """Logic executed after stage execution"""
        passport = (
            DatasetPassportBuilder()
            .phase(self.phase)
            .stage(self.stage)
            .source(source.passport)
            .creator(self.__class__.__name__)
            .name("reviews")
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
