#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/sentiment/stage.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 07:01:12 pm                                              #
# Modified   : Thursday November 7th 2024 10:56:16 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import List

from discover.flow.base.task import Task
from discover.flow.enrich.stage import EnrichmentStage


# ------------------------------------------------------------------------------------------------ #
class SentimentClassificationStage(EnrichmentStage):
    """
    A class that represents a stage in the data processing pipeline dedicated to
    classifying sentiment in textual data.

    This stage is responsible for loading the source data, applying sentiment classification
    tasks, and saving the processed data to the specified destination. It also generates a
    unique asset ID for the destination using the provided configuration details.

    Attributes:
        source_config (dict): Configuration details for loading the source data asset.
        destination_config (dict): Configuration details for saving the classified data asset.
        tasks (List[Task]): A list of tasks to be executed for sentiment classification.
        force (bool): Whether to force the execution of the stage, overriding existing assets.
        **kwargs: Additional keyword arguments for customization and flexibility.

    Methods:
        _load_source_data() -> pd.DataFrame:
            Loads the source dataset from the repository using the source asset ID.
    """

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the SentimentClassificationStage with source and destination configurations,
        a list of tasks for sentiment classification, and an optional force flag.

        Args:
            source_config (dict): Configuration details for the source data asset.
            destination_config (dict): Configuration details for the destination data asset.
            tasks (List[Task]): A list of tasks to execute during the sentiment classification stage.
            force (bool): If True, forces the execution of the stage, even if the destination asset exists.
            **kwargs: Additional keyword arguments for flexibility and customization.
        """
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )
