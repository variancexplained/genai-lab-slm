#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/base/stage.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday November 16th 2024 05:37:04 pm                                             #
# Modified   : Saturday November 16th 2024 05:45:30 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data preparation stage class module."""
from __future__ import annotations

import logging
from abc import abstractmethod
from typing import List, Type, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.flow.base.stage import Stage
from discover.flow.base.task import Task
from discover.infra.persistence.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
#                              DATA PROCESSING STAGE                                               #
# ------------------------------------------------------------------------------------------------ #
class DataProcessingStage(Stage):
    """
    Base class for data processing stages in the data pipeline.

    This class defines the common functionality for all data processing stages, including methods for
    creating, saving, and removing datasets. It manages configuration details, logging, and
    interaction with the dataset repository.

    Attributes:
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        tasks (List[Task]): List of tasks to be executed during the stage.
        asset_idgen (Type[AssetIDGen]): Class responsible for generating asset IDs. Defaults to AssetIDGen.
        repo (DatasetRepo): Repository for managing datasets. Injected from the container.
        force (bool): Flag to force execution even if conditions aren't optimal. Defaults to False.
        _logger (logging.Logger): Logger for the class.
    """

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        asset_idgen: Type[AssetIDGen] = AssetIDGen,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        force: bool = False,
        **kwargs,
    ) -> None:

        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )
        self._asset_idgen = asset_idgen
        self._repo = repo

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the destination dataset."""
        return PhaseDef.from_value(value=self._destination_config.phase)

    @property
    def stage(self) -> PhaseDef:
        """Returns the stage of the destination dataset."""
        return StageDef.from_value(value=self._destination_config.stage)

    @abstractmethod
    def run(self) -> str:
        """Returns the dataset asset_id"""

    def _dataset_exists(self, asset_id: str) -> bool:
        """Checks if the dataset exists in the repository."""
        try:
            return self._repo.exists(asset_id=asset_id)
        except Exception as e:
            msg = f"Error checking dataset {asset_id} existence.\n{str(e)}"
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _create_dataset(
        self,
        asset_id: str,
        config: NestedNamespace,
        data: Union[pd.DataFrame, pyspark.sql.DataFrame],
    ) -> Dataset:
        """
        Creates a dataset.

        Args:
            asset_id (str): The identifier for the asset.
            config (NestedNamespace): Configuration for the dataset.
            data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The dataset payload.

        Returns:
            Dataset: The created dataset.
        """
        return Dataset(
            phase=PhaseDef.from_value(config.phase),
            stage=StageDef.from_value(config.stage),
            name=config.name,
            content=data,
            nlp=config.nlp,
            distributed=config.distributed,
        )

    def _save_dataset(self, dataset: Dataset) -> None:
        """
        Saves a dataset to the repository.

        Args:
            dataset (Dataset): The dataset to be saved.


        Raises:
            RuntimeError: If saving the dataset fails.
        """
        try:
            self._repo.add(dataset=dataset)
        except Exception as e:
            raise RuntimeError(f"Failed to save dataset\n{dataset} {str(e)}")

    def _remove_dataset(self, asset_id: str) -> None:
        """
        Removes the dataset with the specified asset_id from the repository.

        Args:
            asset_id (str): The asset id for the dataset to be removed.

        Raises:
            DatasetNotFoundError: If the destination dataset cannot be found.
        """
        try:
            self._repo.remove(asset_id=asset_id)
        except Exception as e:
            raise RuntimeError(
                f"Failed to remove dataset with asset_id: {asset_id}\n {str(e)}"
            )
