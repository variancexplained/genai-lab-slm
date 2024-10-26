#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/feature/stage.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:57:59 pm                                              #
# Modified   : Saturday October 26th 2024 02:59:16 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Stage Module"""

import logging
from typing import List

from pyspark.sql import DataFrame

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class FeatureEngineeringStage(DataPrepStage):

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._destination_config.asset_type,
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=DataPrepStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _load_source_data(self) -> DataFrame:
        """Loads the source dataset from the repository using the source asset ID."""
        source_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._source_config.asset_type,
            phase=PhaseDef.from_value(value=self._source_config.phase),
            stage=DataPrepStageDef.from_value(value=self._source_config.stage),
            name=self._source_config.name,
        )
        dataset = self._repo.get(asset_id=source_asset_id, distributed=True, nlp=True)
        # Spark adds a column for the pandas index if it exists, which is renamed here.
        if "__index_level_0__" in dataset.content.columns:
            dataset.content = dataset.content.withColumnRenamed(
                "__index_level_0__", "pandas_index"
            )
        return dataset.content
