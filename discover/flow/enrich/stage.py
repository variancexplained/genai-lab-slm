#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/stage.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Friday November 8th 2024 01:30:38 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from typing import List, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import EnrichmentStageDef, PhaseDef
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger


# ------------------------------------------------------------------------------------------------ #
#                                  ENRICHMENT STAGE                                                #
# ------------------------------------------------------------------------------------------------ #
class EnrichmentStage(Stage):
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
        self._repo = repo

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._destination_config.asset_type,
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=EnrichmentStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the destination configuration."""
        return PhaseDef.from_value(value=self._destination_config.phase)

    @property
    def stage(self) -> PhaseDef:
        """Returns the stage of the destination configuration."""
        return EnrichmentStageDef.from_value(value=self._destination_config.stage)

    @stage_logger
    def run(self) -> str:
        """Executes the stage by loading the source dataset, applying tasks, and saving the result.

        Returns:
            str: The asset ID for the created or updated dataset.
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
                data = task.run(data=data)

            dataset = self._create_destination_dataset(data=data)

            self._save_destination_dataset(dataset=dataset)

            return self._destination_asset_id

    def _endpoint_exists(self, asset_id: str) -> bool:
        """Checks if the dataset endpoint already exists in the repository.

        Args:
            asset_id (str): The asset ID to check in the repository.

        Returns:
            bool: True if the dataset endpoint exists, False otherwise.
        """
        return self._repo.exists(asset_id=asset_id)

    def _load_source_data(self) -> pd.DataFrame:
        """Loads the source dataset from the repository using the source asset ID.

        Returns:
            pd.DataFrame: The content of the source dataset as a Pandas DataFrame.
        """
        source_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._source_config.asset_type,
            phase=PhaseDef.from_value(value=self._source_config.phase),
            stage=EnrichmentStageDef.from_value(value=self._source_config.stage),
            name=self._source_config.name,
        )
        return self._repo.get(
            asset_id=source_asset_id,
            distributed=self._source_config.distributed,
            nlp=self._source_config.nlp,
        ).content

    def _create_destination_dataset(
        self, data: Union[pd.DataFrame, pyspark.sql.DataFrame]
    ) -> Dataset:
        """Creates the destination dataset with the processed data and configuration details.

        Args:
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The processed data to be included in the dataset.

        Returns:
            Dataset: The newly created destination dataset.
        """
        return Dataset(
            phase=PhaseDef.from_value(self._destination_config.phase),
            stage=EnrichmentStageDef.from_value(self._destination_config.stage),
            name=self._destination_config.name,
            content=data,
            nlp=self._destination_config.nlp,
            distributed=self._destination_config.distributed,
        )

    def _remove_destination_dataset(self) -> None:
        """Removes the destination dataset from the repository."""
        self._repo.remove(asset_id=self._destination_asset_id)

    def _save_destination_dataset(self, dataset: Dataset) -> None:
        """Saves the processed dataset to the repository using the destination asset ID.

        Args:
            dataset (Dataset): The processed dataset to save.
        """
        self._repo.add(dataset=dataset)
