#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/clean/stage.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:30:48 am                                              #
# Modified   : Tuesday January 28th 2025 01:13:51 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Stage Module"""
from typing import List, Optional

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.asset.dataset.dataset import Dataset
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.stage import Stage
from genailab.flow.base.task import Task
from genailab.infra.persist.repo.dataset import DatasetRepo
from pyspark.sql import SparkSession

from genailab.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
class DataCleaningStage(Stage):
    """
    Represents the data cleaning stage of the pipeline, responsible for
    performing semi-cleaning operations on the dataset using Spark.

    Attributes:
        phase (PhaseDef): The phase of the pipeline, indicating the current
            stage is part of the data preparation phase.
        stage (StageDef): The specific stage within the pipeline, marked as
            semi-cleaning.
        dftype (DFType): The type of data frame utilized, which is Spark in
            this context.

    Args:
        source_config (Dict[str, str]): Configuration details for the data
            source, typically extracted from a configuration file.
        tasks (List[Task]): A list of tasks defining the operations to be
            performed in this stage.
        state (FlowState): The current state of the data flow, used to manage
            and track progress through the pipeline.
        repo (DatasetRepo): The repository that manages dataset storage and
            retrieval.
        dataset_builder (DatasetBuilder): The builder responsible for
            constructing the dataset.
        spark (Optional[SparkSession]): An optional Spark session to be used
            for Spark operations. Defaults to None.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.SEMICLEAN
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

    def approve(self, dataset: Dataset) -> Dataset:
        """Approves the cleaned Dataset.

        This method elevates the dataset from `semiclean` to 'clean' status,
        removes the anomaly detection annotations, updates the flowstate,
        and persists the dataset in the repository.

        Args:
            dataset (Dataset): Dataset to approve
        """
        # Drop annotation columns from the dataset.
        df = dataset.dataframe.drop(
            list(dataset.dataframe.filter(regex="dqa_")), axis=1
        )

        # Create a configuration for the clean dataset
        config = DatasetConfig(phase=self.phase,
                               stage=StageDef.CLEAN,
                               name="review",
                               file_format=FileFormat.PARQUET,
                               dftype=DFType.SPARK)
        clean_dataset = (DatasetBuilder()
                         .from_config(config)
                         .dataframe(dataframe=df)
                         .source(dataset.passport)
                         .creator(self.__class__.__name__)
                         .build()
        )

        # Delete the dataset if it already exists
        if self._repo.exists(asset_id=clean_dataset.asset_id):
            self._repo.remove(asset_id=clean_dataset.asset_id)

        # Add the dataset to the repository
        self._repo.add(
            asset=clean_dataset, entity=self.__class__.__name__
        )

        return clean_dataset
