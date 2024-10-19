#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/base.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Friday October 18th 2024 05:01:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC
from typing import Union

import pandas as pd
import pyspark
import pyspark.sql
from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.persistence.repo.dataset import DatasetRepo

# ------------------------------------------------------------------------------------------------ #


class Analysis(ABC):
    """
    Base class for performing data analysis tasks. This class provides common
    functionality for loading datasets from a dataset repository and is designed
    to be extended by specific analysis implementations.

    Attributes:
        _repo (DatasetRepo): The dataset repository for interacting with stored datasets.
        _data (Union[pd.DataFrame, pyspark.sql.DataFrame]): The loaded dataset,
            either a Pandas DataFrame or PySpark DataFrame, depending on the data source.

    Args:
        repo (DatasetRepo): The dataset repository instance, injected into the class,
            used to retrieve datasets from storage.
    """

    @inject
    def __init__(
        self, repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo]
    ) -> None:
        """
        Initializes the Analysis class by injecting a dataset repository.

        Args:
            repo (DatasetRepo): The repository for accessing datasets.
        """
        self._repo = repo
        self._data = None

    def _load_data(self, asset_id: str) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Loads data from the dataset repository using the given asset ID.

        Args:
            asset_id (str): The unique identifier of the dataset to be loaded.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: The loaded dataset, which can
            either be a Pandas DataFrame or a PySpark DataFrame depending on the data source.
        """
        return self._repo.get(asset_id=asset_id).content
