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
# Modified   : Thursday January 23rd 2025 08:17:46 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Ingestion Stage Module"""
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
        """Returns a boolean indicating whether a fresh cache of the target is extant.

        A fresh cache, for the ingest stage, is target that exists.

        Returns:
            bool: True if the above condition is met, False otherwise.

        """
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
        # Get the source dataframe from file
        dataframe = self._fao.read(
            filepath=self._source_config.filepath,
            file_format=self._source_config.file_format,
            dftype=self._source_config.dftype,
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
        target = self._repo.add(dataset=target)

        return target

    def _create_dataset(
        self,
        config: DatasetConfig,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
        **kwargs,
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
            .dataframe(dataframe=dataframe)
            .build()
        )
