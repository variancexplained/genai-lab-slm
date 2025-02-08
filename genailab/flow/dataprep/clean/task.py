#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/clean/task.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday February 8th 2025 12:14:33 pm                                              #
# Modified   : Saturday February 8th 2025 12:38:52 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Dataset Cleaner Module"""
from dependency_injector.wiring import Provide, inject

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.asset.dataset.dataset import Dataset
from genailab.container import GenAILabContainer
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.infra.persist.repo.dataset import DatasetRepo
from genailab.infra.service.spark.pool import SparkSessionPool
from genailab.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
class DatasetFinalCleaner:
    @inject
    def __init__(self, repo: DatasetRepo = Provide[GenAILabContainer.io.repo], spark_session_pool: SparkSessionPool = Provide[GenAILabContainer.spark.session_pool]):
        super().__init__()
        self._repo = repo
        self._spark_session_pool = spark_session_pool

    def run(self, asset_id: str) -> Dataset:
        """Performs final cleaning of the dataset.

        This method removes all the data quality analysis columns and annotations and
        and persists the dataset in the repository.

        Args:
            asset_id (str): Dataset asset_id
        """
        # Obtain a spark session and the pyspark dataset
        spark = self._spark_session_pool.spark
        dataset = self._repo.get(asset_id=asset_id, dftype=DFType.SPARK, spark=spark)

        # Drop annotation columns from the dataset.
        cols_to_keep = [col for col in dataset.dataframe.columns if not col.startswith("dqa_")]
        df = dataset.dataframe.select(*cols_to_keep)

        # Create a configuration for the clean dataset
        config = DatasetConfig(phase=PhaseDef.DATAPREP,
                               stage=StageDef.CLEAN,
                               name="review",
                               file_format=FileFormat.PARQUET,
                               dftype=DFType.SPARK)

        # Get the asset_id for the new dataset and remove it from the repo if it already exists
        asset_id = self._repo.get_asset_id(phase=config.phase, stage=config.stage, name=config.name)
        if self._repo.exists(asset_id=asset_id):
            self._repo.remove(asset_id=asset_id)

        # Create the clean dataset
        clean_dataset = (DatasetBuilder()
                         .from_config(config)
                         .dataframe(dataframe=df)
                         .source(dataset.passport)
                         .creator(self.__class__.__name__)
                         .build()
        )

        # Publish to repository
        self._repo.add(
            dataset=clean_dataset, entity=self.__class__.__name__
        )

        return clean_dataset