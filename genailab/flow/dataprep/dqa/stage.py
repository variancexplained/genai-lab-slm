#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/flow/dataprep/dqa/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:30:48 am                                              #
# Modified   : Saturday January 25th 2025 04:41:11 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Stage Module"""
from typing import List, Optional

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.stage import Stage
from genailab.flow.base.task import Task
from genailab.infra.persist.repo.dataset import DatasetRepo
from pyspark.sql import SparkSession


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
