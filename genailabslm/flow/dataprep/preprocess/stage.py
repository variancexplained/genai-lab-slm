#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/flow/dataprep/preprocess/stage.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:30:48 am                                              #
# Modified   : Sunday January 26th 2025 06:17:32 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Preprocessing Stage Module"""
from typing import List, Optional

from genailabslm.asset.dataset.builder import DatasetBuilder
from genailabslm.asset.dataset.config import DatasetConfig
from genailabslm.core.dtypes import DFType
from genailabslm.core.flow import PhaseDef, StageDef
from genailabslm.flow.base.stage import Stage
from genailabslm.flow.base.task import Task
from genailabslm.infra.persist.repo.dataset import DatasetRepo
from pyspark.sql import SparkSession


# ------------------------------------------------------------------------------------------------ #
class PreprocessStage(Stage):
    """
    Represents the preprocess stage of a data pipeline.

    This stage is responsible for reading source data, executing configured tasks,
    and saving the processed dataset to the target. It defines the specific phase,
    stage, and dataframe type used in the pipeline.

    Attributes:
        phase (PhaseDef): The phase of the pipeline, set to `PhaseDef.DATAPREP`.
        stage (StageDef): The stage of the pipeline, set to `StageDef.PREPROCESS`.
        dftype (DFType): The dataframe type of the pipeline, set to `DFType.PANDAS`.

    Args:
        source_config (DatasetConfig): Configuration of the source dataset.
        target_config (DatasetConfig): Configuration of the target dataset.
        tasks (List[Task]): List of tasks to execute on the dataset.
        repo (DatasetRepo): Repository for managing datasets.
        dataset_builder (DatasetBuilder): Builder for creating `Dataset` objects.
        spark (Optional[SparkSession]): Optional Spark session for distributed processing.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.PREPROCESS
    __DFTYPE = DFType.PANDAS

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
        """
        Defines the phase of the pipeline.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """
        Defines the stage of the pipeline.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """
        Defines the dataframe type of the pipeline.

        Returns:
            DFType: The dataframe type used in the pipeline.
        """
        return self.__DFTYPE
