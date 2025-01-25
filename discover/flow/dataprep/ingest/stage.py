#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/ingest/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:30:48 am                                              #
# Modified   : Friday January 24th 2025 10:25:02 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Ingestion Stage Module"""
import os
from typing import List, Optional, Union

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.config import DatasetConfig, FilesetConfig
from discover.asset.dataset.dataset import Dataset
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.file.fao import FAO


# ------------------------------------------------------------------------------------------------ #
class IngestStage(Stage):
    """
    Represents the ingest stage of a data pipeline.

    This stage is responsible for reading source data, executing configured tasks,
    and saving the processed dataset to the target. It defines the specific phase,
    stage, and dataframe type used in the pipeline.

    Attributes:
        phase (PhaseDef): The phase of the pipeline, set to `PhaseDef.DATAPREP`.
        stage (StageDef): The stage of the pipeline, set to `StageDef.INGEST`.
        dftype (DFType): The dataframe type of the pipeline, set to `DFType.PANDAS`.

    Args:
        source_config (FilesetConfig): Configuration of the source dataset.
        target_config (DatasetConfig): Configuration of the target dataset.
        tasks (List[Task]): List of tasks to execute on the dataset.
        repo (DatasetRepo): Repository for managing datasets.
        fao (FAO): File Access Object for reading and writing data.
        dataset_builder (DatasetBuilder): Builder for creating `Dataset` objects.
        spark (Optional[SparkSession]): Optional Spark session for distributed processing.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.INGEST
    __DFTYPE = DFType.PANDAS

    def __init__(
        self,
        source_config: FilesetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        repo: DatasetRepo,
        fao: FAO,
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
        self._fao = fao

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

    def _fresh_cache_exists(self) -> bool:
        """
        Determines whether a fresh cache of the target dataset exists.

        A fresh cache, for the ingest stage, is defined as the existence
        of the target dataset in the repository.

        Returns:
            bool: True if the target dataset exists, False otherwise.
        """
        target_asset_id = self._repo.get_asset_id(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        return self._repo.exists(asset_id=target_asset_id)

    def _run(self) -> Dataset:
        """
        Executes the ingest stage by reading source data, processing it with tasks,
        and saving the processed dataset to the target.

        Returns:
            Dataset: The processed dataset saved in the repository.

        Raises:
            RuntimeError: If a task fails during execution.
        """
        # Remove the target if it exists.
        self._remove_dataset(
            phase=self._target_config.phase,
            stage=self._target_config.stage,
            name=self._target_config.name,
        )
        # Get the source dataframe from file
        dataframe = self._fao.read(
            filepath=self._source_config.filepath,
            file_format=self._source_config.file_format,
            dftype=self.dftype,
            spark=self._spark,
        )

        # Process
        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        # Create target dataset and publish to repository
        target = self._create_dataset(config=self._target_config, dataframe=dataframe)
        target = self._repo.add(dataset=target, entity=self.__class__.__name__)

        return target

    def _create_dataset(
        self,
        config: DatasetConfig,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
        **kwargs,
    ) -> Dataset:
        """
        Creates a `Dataset` object based on the given configuration and dataframe.

        Args:
            config (DatasetConfig): Configuration for the target dataset.
            dataframe (Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame]):
                The dataframe content to store in the dataset.

        Returns:
            Dataset: The created dataset object.
        """
        return (
            self._dataset_builder.from_config(config=config)
            .creator(creator=self.__class__.__name__)
            .dataframe(dataframe=dataframe)
            .build()
        )

    def _source_exists(self, config: FilesetConfig) -> bool:
        """Checks existence of a dataset, given its configuration."""
        return os.path.exists(config.filepath)
