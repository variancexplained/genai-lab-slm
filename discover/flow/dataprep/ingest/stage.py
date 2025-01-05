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
# Modified   : Saturday January 4th 2025 11:48:28 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Ingestion Stage Module"""
from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from discover.asset.dataset.builder import DatasetBuilder, DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class IngestStage(Stage):

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.INGEST
    __DFTYPE = DFType.PANDAS

    def __init__(
        self,
        source_config: Dict[str, str],
        tasks: List[Task],
        state: FlowState,
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        spark: Optional[SparkSession] = None,
    ) -> None:
        super().__init__(
            source_config=source_config,
            tasks=tasks,
            state=state,
            repo=repo,
            dataset_builder=dataset_builder,
            spark=spark,
        )

    @property
    def phase(self) -> PhaseDef:
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        return self.__DFTYPE

    def get_source_dataset(self, **kwargs) -> Dataset:
        """Logic executed prior at the onset of stage execution"""
        source_phase = PhaseDef.from_value(self._source_config["phase"])
        source_stage = StageDef.from_value(self._source_config["stage"])
        passport = (
            DatasetPassportBuilder()
            .phase(source_phase)
            .stage(source_stage)
            .creator(self.__class__.__name__)
            .name("reviews")
            .build()
            .passport
        )
        source = (
            self._dataset_builder.from_file(self._source_config)
            .passport(passport)
            .to_parquet()
            .build()
            .dataset
        )
        return source
