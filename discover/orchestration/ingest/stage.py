#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/ingest/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 01:00:03 am                                              #
# Modified   : Thursday October 17th 2024 02:33:23 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import List

import pandas as pd
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.utils.file.io import IOService
from discover.orchestration.base.stage import Stage
from discover.orchestration.base.task import Task


# ------------------------------------------------------------------------------------------------ #
class IngestStage(Stage):

    @inject
    def __init__(
        self,
        source_filepath: str,
        destination_config: dict,
        tasks: List[Task],
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        force: bool = False,
    ) -> None:
        self._source_filepath = source_filepath
        self._destination_config = destination_config
        self._tasks = tasks
        self._repo = repo
        self._force = force

        self._source_data = None

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=DataPrepStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

    def run(self) -> None:
        if not self._endpoint_exists(self._destination_asset_id) or self._force:
            self._setup()
            data = self._source_data
            for task in self._tasks:
                data = task.run(data=data)
            self._teardown(data=data)

    def _setup(self) -> None:
        self._load_source_data()

    def _teardown(self, data: pd.DataFrame) -> None:
        dataset = Dataset(
            phase=PhaseDef.from_value(self._destination_config.phase),
            stage=DataPrepStageDef.from_value(self._destination_config.stage),
            name=self._destination_config.name,
            content=data,
            nlp=self._destination_config.nlp,
            distributed=self._destination_config.distributed,
        )
        self._save_destination_dataset(dataset=dataset)

    def _load_source_data(self) -> None:
        self._source_data = IOService.read(filepath=self._source_filepath)

    def _save_destination_dataset(self, dataset) -> None:
        self._repo.add(dataset=dataset)

    def _endpoint_exists(self, asset_id: str) -> bool:
        """Checks existence of the data endpoint."""
        return self._repo.exists(asset_id=asset_id)
