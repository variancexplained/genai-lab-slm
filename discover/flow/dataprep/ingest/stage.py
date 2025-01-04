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
# Modified   : Friday January 3rd 2025 07:23:51 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Ingestion Stage Module"""
from typing import List, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset.builder import DatasetBuilder, DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo


class IngestStage(Stage):
    def __init__(
        self,
        source_filepath: str,
        tasks: List[Task],
        state: FlowState,
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
    ) -> None:
        super().__init__(
            tasks=tasks, dataset_builder=dataset_builder, state=state, repo=repo
        )
        self._source_filepath = source_filepath

    def get_source_data(self) -> Dataset:
        """Logic executed prior at the onset of stage execution"""
        passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .creator(self.__class__.__name__)
            .name("reviews")
            .build()
            .passport
        )
        source = (
            self._dataset_builder.from_source_filepath(self._source_filepath)
            .passport(passport)
            .as_pandas()
            .to_parquet()
            .build()
            .dataset
        )
        return source

    def save_target_data(self, data: Union[pd.DataFrame, DataFrame]) -> Dataset:
        """Logic executed after stage execution"""
        passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .source(self._source.passport)
            .creator(self.__class__.__name__)
            .name("reviews")
            .build()
            .passport
        )
        target = (
            self._dataset_builder.from_dataframe(data)
            .passport(passport)
            .as_pandas()
            .to_parquet()
            .build()
            .dataset
        )
        self._repo.add(asset=target)
        self._state.create(passport=target.passport)
        return target
