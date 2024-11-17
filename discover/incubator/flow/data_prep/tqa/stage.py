#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/incubator/flow/data_prep/tqa/stage.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:59:20 pm                                              #
# Modified   : Saturday November 16th 2024 05:46:59 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import List

import pandas as pd

from discover.assets.idgen import AssetIDGen
from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.task import Task
from discover.flow.data_processing.data_prep.stage import DataPrepStage
from discover.infra.service.logging.stage import stage_logger

# ------------------------------------------------------------------------------------------------ #


class TQAStage(DataPrepStage):
    """"""

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
            stage=StageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @stage_logger
    def run(self) -> str:
        """Executes the stage by loading the source dataset, applying tasks, and saving the result.

        Returns:
            asset_id (str): Returns the asset_id for the asset created.
        """
        if (
            self._endpoint_exists(asset_id=self._destination_asset_id)
            and not self._force
        ):
            return self._destination_asset_id
        else:
            if self._repo.exists(asset_id=self._destination_asset_id):
                self._repo.remove(asset_id=self._destination_asset_id)

            data = self._load_source_data()
            results = []

            for task in self._tasks:
                result = task.run(data=data)
                results.append(result)

            data = self._merge_data_results(data=data, results=results)

            dataset = self._create_destination_dataset(data=data)

            self._save_destination_dataset(dataset=dataset)

            return self._destination_asset_id

    def _merge_data_results(self, data: pd.DataFrame, results: list) -> pd.DataFrame:
        """Merges the data with the results of the data quality assessment."""
        results = pd.concat(results, axis=1)
        data = pd.concat((data, results), axis=1)
        return data
