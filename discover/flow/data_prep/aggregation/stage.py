#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/aggregation/stage.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:59:53 pm                                              #
# Modified   : Monday November 11th 2024 03:46:51 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from typing import List, Union

import pandas as pd
import pyspark

from discover.assets.dataset import Dataset
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                  ENRICHMENT STAGE                                                #
# ------------------------------------------------------------------------------------------------ #
class AggregationStage(DataPrepStage):

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the EnrichmentStage with source and destination configurations,
        a list of tasks for data enrichment, and a repository for dataset management.

        Args:
            source_config (dict): Configuration details for the source data asset.
            destination_config (dict): Configuration details for the destination data asset.
            tasks (List[Task]): A list of tasks to execute during the enrichment stage.
            force (bool): If True, forces the execution of the stage, even if the destination asset exists.
            repo (DatasetRepo): A repository for accessing and managing datasets.
            **kwargs: Additional keyword arguments for customization and flexibility.
        """
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )

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

            for task in self._tasks:
                agg = task.run(data=data)
                dataset = self._create_destination_dataset(
                    data=agg, dataset_name=task.dataset_name
                )
                self._save_destination_dataset(dataset=dataset)

            return self._destination_asset_id

    def _create_destination_dataset(
        self, data: Union[pd.DataFrame, pyspark.sql.DataFrame], dataset_name: str
    ) -> Dataset:
        """Creates the destination dataset with the processed data and configuration details."""
        return Dataset(
            phase=PhaseDef.from_value(self._destination_config.phase),
            stage=DataPrepStageDef.from_value(self._destination_config.stage),
            name=dataset_name,
            content=data,
            nlp=self._destination_config.nlp,
            distributed=self._destination_config.distributed,
        )
