#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/dqa/stage.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:30:48 am                                              #
# Modified   : Thursday January 23rd 2025 07:08:09 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Stage Module"""
from typing import List, Optional

from pyspark.sql import SparkSession

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.config import DatasetConfig
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentStage(Stage):
    """
    Represents a stage in the data pipeline focused on data quality checks. This stage is responsible for
    validating and ensuring the integrity of data before further processing.

    Attributes:
        __PHASE (PhaseDef): Defines the phase of the data pipeline, set to DATAPREP.
        __STAGE (StageDef): Defines the stage of the data pipeline, set to Data Quality Assessment (DQA).
        __DFTYPE (DFType): Specifies the data frame type used in this stage, set to SPARK.

    Args:
        source_config (Dict[str, str]): Configuration dictionary containing source-specific settings.
        tasks (List[Task]): A list of tasks to be executed as part of this stage.
        state (FlowState): The current state of the data flow.
        repo (DatasetRepo): Repository object for dataset management.
        dataset_builder (DatasetBuilder): Object responsible for building datasets.
        spark (Optional[SparkSession]): Optional Spark session used for executing tasks on Spark dataframes.

    Properties:
        phase (PhaseDef): Returns the phase of the pipeline, DATAPREP.
        stage (StageDef): Returns the stage of the pipeline, DQC.
        dftype (DFType): Returns the data frame type used, SPARK.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.DQA
    __DFTYPE = DFType.SPARK

    def __init__(
        self,
        source_config: DatasetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        spark: Optional[SparkSession] = None,
    ) -> None:
        super().__init__(
            source_config=source_config,
            target_config=target_config,
            tasks=tasks,
            repo=repo,
            dataset_builder=dataset_builder,
            spark=spark,
        )

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline."""
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage of the pipeline."""
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the data frame type used in this stage."""
        return self.__DFTYPE
