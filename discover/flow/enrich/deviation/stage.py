#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/deviation/stage.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:01:19 pm                                              #
# Modified   : Friday November 8th 2024 12:05:42 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from typing import List

from dependency_injector.wiring import inject

from discover.flow.base.task import Task
from discover.flow.enrich.stage import EnrichmentStage


# ------------------------------------------------------------------------------------------------ #
class DeviationStage(EnrichmentStage):
    """
    A class representing a specialized data enrichment stage that focuses on
    computing deviations and enhancing data with additional metrics.

    This stage is a subclass of EnrichmentStage and is responsible for initializing
    configurations, tasks, and a dataset repository. It manages the execution of
    deviation-based tasks on the data and handles the setup required for data enrichment.

    Attributes:
        source_config (dict): Configuration details for loading the source data asset.
        destination_config (dict): Configuration details for saving the enriched data asset.
        tasks (List[Task]): A list of tasks to be executed for data enrichment.
        force (bool): Whether to force the execution of the stage, overriding existing assets.
        repo (DatasetRepo): A repository object for accessing and managing datasets.
        **kwargs: Additional keyword arguments for customization and flexibility.
    """

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the EnrichmentDeviationStage with source and destination configurations,
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
