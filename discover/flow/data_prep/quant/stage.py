#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/quant/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 10:15:10 pm                                              #
# Modified   : Monday November 11th 2024 04:41:47 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import List

from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage

# ------------------------------------------------------------------------------------------------ #


class QuantStage(DataPrepStage):
    """
    A class that represents a stage in the data processing pipeline dedicated to enriching
    metadata features, such as review length and review age, in the dataset.

    This stage handles loading the source data, applying specified tasks, and saving the
    enriched data to the destination. It also generates a unique asset ID for the destination
    using configuration details.

    Attributes:
        source_config (dict): Configuration details for loading the source data asset.
        destination_config (dict): Configuration details for saving the enriched data asset.
        tasks (List[Task]): A list of tasks to be executed for metadata enrichment.
        force (bool): Whether to force the execution of the stage, overriding existing assets.
        **kwargs: Additional keyword arguments for customization and flexibility.

    Methods:
        _load_source_data() -> DataFrame:
            Loads the source dataset from the repository using the source asset ID.
            If a pandas index column is present in the dataset, it is renamed to "pandas_index".
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
        Initializes the MetadataEnrichmentStage with source and destination configurations,
        a list of tasks for metadata enrichment, and an optional force flag.

        Args:
            source_config (dict): Configuration details for the source data asset.
            destination_config (dict): Configuration details for the destination data asset.
            tasks (List[Task]): A list of tasks to execute during the metadata enrichment stage.
            force (bool): If True, forces the execution of the stage, even if the destination asset exists.
            **kwargs: Additional keyword arguments for flexibility and customization.
        """
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )
