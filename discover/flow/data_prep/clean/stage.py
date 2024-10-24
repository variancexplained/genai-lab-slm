#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/clean/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:57:59 pm                                              #
# Modified   : Wednesday October 23rd 2024 11:37:37 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Stage Module"""

import logging
from typing import List

import pandas as pd

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class DataCleaningStage(DataPrepStage):

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

    def _load_source_data(self) -> pd.DataFrame:
        """Obtains source data from file."""
        return IOService.read(filepath=self._source_config.filepath)
