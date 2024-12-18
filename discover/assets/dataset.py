#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/dataset.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Wednesday December 18th 2024 05:27:48 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from typing import Optional, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.assets.base import Asset
from discover.container import DiscoverContainer
from discover.core.data_structure import DataFrameType
from discover.infra.persistence.repo.fileset import FilesetRepo


# ------------------------------------------------------------------------------------------------ #
#                                      DATASET                                                     #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):
    """Represents a dataset as an asset, supporting multiple representations.

    This class provides methods to manage dataset metadata, load and save data,
    and convert between different representations such as Pandas, Spark, and SparkNLP.

    Args:
        asset_id (str): Unique identifier for the dataset.
        name (str): Name of the dataset.
        description (Optional[str]): Optional description of the dataset.
        data (Optional[Union[pd.DataFrame, DataFrame]]): Initial data for the dataset.
        repo (FilesetRepo): Repository for managing dataset persistence.
        creator (Optional[str]): The creator of the dataset.
    """

    @inject
    def __init__(
        self,
        asset_id: str,
        name: str,
        description: Optional[str] = None,
        data: Optional[Union[pd.DataFrame, DataFrame]] = None,
        repo: FilesetRepo = Provide[DiscoverContainer.persistence.fileset_repo],
        creator: Optional[str] = None,
    ) -> None:
        super().__init__(asset_id=asset_id, name=name, description=description)
        self._data = data
        self._creator = creator
        self._description = (
            self._description
            or f"Dataset asset {self._asset_id} - {self.name} created by {creator} on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        )
        self._persisted = repo.persisted(asset_id=asset_id)
        self._filepath = repo.get_filepath(asset_id=asset_id)
        self._repo = repo

    # --------------------------------------------------------------------------------------------- #
    #                                      SERIALIZATION                                            #
    # --------------------------------------------------------------------------------------------- #
    def __getstate__(self) -> dict:
        """
        Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        # Exclude non-serializable or private attributes if necessary
        state = self.__dict__.copy()
        state["_repo"] = None  # Exclude the repo
        state["_data"] = None  # Exclude data
        return state

    def __setstate__(self, state) -> None:
        """
        Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)

    # --------------------------------------------------------------------------------------------- #
    #                                    FILESET REPO                                               #
    # --------------------------------------------------------------------------------------------- #
    @property
    def repo(self) -> FilesetRepo:
        """Returns the repository."""
        if not self._repo:
            raise ValueError("Repository is not set.")
        return self._repo

    @repo.setter
    def repo(self, repo: FilesetRepo) -> None:
        """Sets the repository."""
        self._repo = repo
        self._persisted = repo.persisted(asset_id=self._asset_id)
        self._filepath = repo.get_filepath(asset_id=self._asset_id)

    # --------------------------------------------------------------------------------------------- #
    #                                    EXTRACT DATA                                               #
    # --------------------------------------------------------------------------------------------- #
    def to_pandas(self) -> pd.DataFrame:
        """Converts the dataset to a Pandas DataFrame.

        Returns:
            pd.DataFrame: The dataset in Pandas format.
        """
        if isinstance(self._data, (pd.DataFrame, pd.core.Frame.DataFrame)):
            return self._data
        else:
            self._load_data(dataframe_type=DataFrameType.PANDAS)
            return self._data

    def to_spark(self) -> DataFrame:
        """Converts the dataset to a Spark DataFrame.

        Returns:
            DataFrame: The dataset in Spark format.
        """
        if self._dataframe_type == DataFrameType.SPARK and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            self._load_data(dataframe_type=DataFrameType.SPARK)
            return self._data

    def to_sparknlp(self) -> DataFrame:
        """Converts the dataset to a SparkNLP DataFrame.

        Returns:
            DataFrame: The dataset in SparkNLP format.
        """
        if self._dataframe_type == DataFrameType.SPARKNLP and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            self._load_data(dataframe_type=DataFrameType.SPARKNLP)
            return self._data

    # --------------------------------------------------------------------------------------------- #
    #                                        DATA IO                                                #
    # --------------------------------------------------------------------------------------------- #
    def load_data(self, dataframe_type: DataFrameType) -> None:
        """Loads data into the dataset from the repository.

        Args:
            dataframe_type (DataFrameType): The type of DataFrame to load (e.g., Pandas, Spark, SparkNLP).
        """
        self._data = self._repo.get(
            asset_id=self._asset_id, dataframe_type=dataframe_type
        )
        self._dataframe_type = dataframe_type

    def save_data(self) -> None:
        """Saves the dataset to the repository.

        Raises:
            RuntimeError: If no data is available to save.
        """
        if self._data is None:
            raise RuntimeError("No Data to save.")
        self._repo.add(asset_id=self._asset_id, data=self._data)

    # --------------------------------------------------------------------------------------------- #
    #                                    FACTORY METHODS                                            #
    # --------------------------------------------------------------------------------------------- #
    @classmethod
    def from_pandas(
        cls,
        asset_id: str,
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Dataset:
        """Creates a dataset from a Pandas DataFrame.

        Args:
            asset_id (str): Unique identifier for the dataset.
            name (str): Name of the dataset.
            data (pd.DataFrame): Pandas DataFrame to initialize the dataset.
            description (Optional[str]): Optional description of the dataset.
            dataframe_type (DataFrameType): The type of the DataFrame (defaults to PANDAS).

        Returns:
            Dataset: An instance of the Dataset class.
        """
        return cls(
            asset_id=asset_id,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )

    @classmethod
    def from_spark(
        cls,
        asset_id: str,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.SPARK,
    ) -> Dataset:
        """Creates a dataset from a Spark DataFrame.

        Args:
            asset_id (str): Unique identifier for the dataset.
            name (str): Name of the dataset.
            data (DataFrame): Spark DataFrame to initialize the dataset.
            description (Optional[str]): Optional description of the dataset.
            dataframe_type (DataFrameType): The type of the DataFrame (defaults to SPARK).

        Returns:
            Dataset: An instance of the Dataset class.
        """
        return cls(
            asset_id=asset_id,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )

    @classmethod
    def from_sparknlp(
        cls,
        asset_id: str,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        dataframe_type: DataFrameType = DataFrameType.SPARKNLP,
    ) -> Dataset:
        """Creates a dataset from a SparkNLP DataFrame.

        Args:
            asset_id (str): Unique identifier for the dataset.
            name (str): Name of the dataset.
            data (DataFrame): SparkNLP DataFrame to initialize the dataset.
            description (Optional[str]): Optional description of the dataset.
            dataframe_type (DataFrameType): The type of the DataFrame (defaults to SPARKNLP).

        Returns:
            Dataset: An instance of the Dataset class.
        """
        return cls(
            asset_id=asset_id,
            name=name,
            data=data,
            description=description,
            dataframe_type=dataframe_type,
        )
