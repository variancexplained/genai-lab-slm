#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/tqa/stage.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:01:19 pm                                              #
# Modified   : Monday November 11th 2024 04:08:45 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from typing import List

from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class TQAStage(DataPrepStage):
    """
    A class that represents a stage in the data enrichment pipeline, responsible for enhancing
    and processing data through a series of tasks.

    This class handles loading the source data, applying a series of enrichment tasks, and
    saving the enriched data to the destination. It manages asset IDs and ensures that
    previously existing data can be removed if necessary.

    Attributes:
        source_config (dict): Configuration details for the source data asset.
        destination_config (dict): Configuration details for the destination data asset.
        tasks (List[Task]): A list of tasks to be executed for data enrichment.
        force (bool): Whether to force the execution of the stage, even if the destination asset already exists.
        repo (DatasetRepo): A repository object for accessing and managing datasets.
        **kwargs: Additional keyword arguments for customization and flexibility.

    Methods:
        phase() -> PhaseDef:
            Property that returns the phase of the destination configuration.

        stage() -> PhaseDef:
            Property that returns the stage of the destination configuration.

        run() -> str:
            Executes the enrichment stage by loading the source data, applying the tasks,
            and saving the enriched data to the destination. Returns the asset ID of the
            created or updated dataset.

        _endpoint_exists(asset_id: str) -> bool:
            Checks if the dataset endpoint already exists in the repository.

        _load_source_data() -> pd.DataFrame:
            Loads the source dataset from the repository using the source asset ID.

        _create_destination_dataset(data: Union[pd.DataFrame, pyspark.sql.DataFrame]) -> Dataset:
            Creates the destination dataset with the processed data and configuration details.

        _remove_destination_dataset() -> None:
            Removes the destination dataset from the repository.

        _save_destination_dataset(dataset: Dataset) -> None:
            Saves the processed dataset to the repository using the destination asset ID.
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
