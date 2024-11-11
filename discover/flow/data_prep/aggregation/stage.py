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
# Modified   : Monday November 11th 2024 05:08:49 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from typing import List

from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage
from discover.infra.persistence.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
#                                  ENRICHMENT STAGE                                                #
# ------------------------------------------------------------------------------------------------ #
class AggregationStage(DataPrepStage):

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
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
