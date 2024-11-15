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
# Modified   : Friday November 15th 2024 12:21:42 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
# ------------------------------------------------------------------------------------------------ #
from abc import ABC
from typing import Type

import pandas as pd
from dependency_injector.wiring import Provide, inject

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.infra.config.app import AppConfigReader
from discover.infra.persistence.repo.dataset import DatasetRepo


class DataManager(ABC):
    """Abstract base class for managing data operations, including dataset retrieval,
    addition, and asset ID generation.

    Attributes:
        _repo: The dataset repository used for retrieving and adding datasets.
        _config_reader: An instance of the configuration reader for accessing environment settings.
        _env: The current environment obtained from the configuration reader.
    """

    @inject
    def __init__(
        self,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        config_reader_cls: Type["AppConfigReader"] = AppConfigReader,
    ) -> None:
        """Initializes the DataManager with a repository and configuration reader.

        Args:
            repo (DatasetRepo): The dataset repository for data retrieval and addition.
            config_reader_cls (Type[AppConfigReader], optional): Class to read configuration settings. Defaults to AppConfigReader.

        Initializes:
            - The dataset repository (`_repo`).
            - The configuration reader (`_config_reader`).
            - The current environment (`_env`).
        """
        self._repo = repo
        self._config_reader = config_reader_cls()
        self._env = self._config_reader.get_environment()

    def get_dataset(self, stage: str = "ingest", name: str = "review") -> pd.DataFrame:
        """Retrieves a dataset from the repository based on the given stage and name.

        Args:
            stage (str, optional): The stage of the dataset. Defaults to "ingest".
            name (str, optional): The name of the dataset. Defaults to "review".

        Returns:
            pd.DataFrame: The content of the retrieved dataset.
        """
        asset_id = self.get_asset_id(stage=stage, name=name)
        return self._repo.get(asset_id=asset_id).content

    def add_dataset(self, df: pd.DataFrame, stage: str = "sentiment") -> None:
        """Adds a dataset to the repository.

        Args:
            df (pd.DataFrame): The dataset to add.
            stage (str, optional): The stage of the dataset. Defaults to "sentiment".

        Returns:
            None
        """
        dataset = Dataset(
            phase=PhaseDef.DATAPREP,
            stage=DataPrepStageDef.from_value(value=stage),
            name="review",
            content=df,
            nlp=False,
            distributed=False,
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
            stage=DataPrepStageDef.from_value(value=stage),
            name=name,
        )
