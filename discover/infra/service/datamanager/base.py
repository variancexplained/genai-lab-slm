#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/datamanager/base.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 14th 2024 11:59:29 pm                                             #
# Modified   : Sunday December 15th 2024 04:31:13 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataManager Module"""
from abc import ABC
from typing import Type

import pandas as pd
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader
from discover.infra.persistence.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class DataManager(ABC):
    """
    Abstract base class for managing datasets in a data pipeline.

    Provides methods to retrieve and add datasets, interacting with the underlying repository
    and configuration settings.

    Args:
        repo (DatasetRepo): Repository for dataset operations.
        config_reader_cls (Type[AppConfigReader]): Class for reading application configuration.

    Attributes:
        _repo (DatasetRepo): Handles dataset persistence and retrieval operations.
        _config_reader (AppConfigReader): Reads application configuration settings.
        _env (str): The current environment determined from the application configuration.
    """

    @inject
    def __init__(
        self,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        config_reader_cls: Type["AppConfigReader"] = AppConfigReader,
    ) -> None:
        self._repo = repo
        self._config_reader = config_reader_cls()
        self._env = self._config_reader.get_environment()

    def get_dataset(
        self,
        stage: str = "ingest",
        name: str = "review",
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> pd.DataFrame:
        """
        Retrieves a dataset by stage and name, returning its content as a DataFrame.

        Args:
            stage (str): The pipeline stage of the dataset. Defaults to "ingest".
            name (str): The name of the dataset. Defaults to "review".
            dataframe_type (DataFrameType): Type of DataFrame to return. Defaults to PANDAS.

        Returns:
            pd.DataFrame: The dataset content as a DataFrame.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an error occurs during dataset retrieval.
        """
        asset_id = self.get_asset_id(stage=stage, name=name)
        return self._repo.get(asset_id=asset_id, dataframe_type=dataframe_type).content

    def add_dataset(
        self,
        df: pd.DataFrame,
        stage: str = "sentiment",
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> None:
        """
        Adds a dataset to the repository.

        Args:
            df (pd.DataFrame): The dataset content to persist.
            stage (str): The pipeline stage of the dataset. Defaults to "sentiment".
            dataframe_type (DataFrameType): Type of DataFrame provided. Defaults to PANDAS.

        Raises:
            DatasetIOError: If an error occurs during dataset persistence.
            DatasetCreationError: If dataset creation fails.
        """
        dataset = Dataset(
            phase=PhaseDef.DATAPREP,
            stage=StageDef.from_value(value=stage),
            name="review",
            content=df,
            dataframe_type=dataframe_type,
        )
        self._repo.add(dataset=dataset)

    def dataset_exists(self, stage: str, name: str = "review") -> bool:
        """Checks if a dataset with the specified stage and name exists in the repository.

        Args:
            stage (str): The stage of the dataset.
            name (str, optional): The name of the dataset. Defaults to "review".

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        asset_id = self.get_asset_id(stage=stage, name=name)
        return self._repo.exists(asset_id=asset_id)

    def get_asset_id(self, stage: str, name: str = "review") -> str:
        """Generates an asset ID for the specified stage and name.

        Args:
            stage (str): The stage of the dataset.
            name (str, optional): The name of the dataset. Defaults to "review".

        Returns:
            str: The generated asset ID.
        """
        return AssetIDGen().get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=StageDef.from_value(value=stage),
            name=name,
        )
