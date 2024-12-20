#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/data/dataset.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Friday December 20th 2024 12:45:27 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from typing import Optional, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.assets.base import Asset
from discover.container import DiscoverContainer
from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                      DATASET                                                     #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):

    def __init__(
        self,
        asset_id: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        description: Optional[str] = None,
        data: Optional[Union[pd.DataFrame, DataFrame]] = None,
        filepath: Optional[str] = None,
        data_structure: Optional[DataStructure] = None,
        source: Optional[str] = None,
        parent: Optional[Dataset] = None,
    ) -> None:
        super().__init__(
            asset_id=asset_id,
            name=name,
            phase=phase,
            stage=stage,
            description=description,
        )
        self._data = data
        self._filepath = filepath
        self._source = source
        self._parent = parent
        self._description = (
            self._description
            or f"Dataset asset {self._asset_id} - {self.name} created by {source} on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        )

        self._repo = None
        self._is_composite = False

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
    #                                    EXTRACT DATA                                               #
    # --------------------------------------------------------------------------------------------- #
    def to_pandas(self) -> pd.DataFrame:
        """Converts the dataset to a Pandas DataFrame.

        Returns:
            pd.DataFrame: The dataset in Pandas format.
        """
        if isinstance(self._data, (pd.DataFrame, pd.core.frame.DataFrame)):
            return self._data
        else:
            self._load_data(data_structure=DataStructure.PANDAS)
            return self._data

    def to_spark(self) -> DataFrame:
        """Converts the dataset to a Spark DataFrame.

        Returns:
            DataFrame: The dataset in Spark format.
        """
        if self._data_structure == DataStructure.SPARK and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            self._load_data(data_structure=DataStructure.SPARK)
            return self._data

    def to_sparknlp(self) -> DataFrame:
        """Converts the dataset to a SparkNLP DataFrame.

        Returns:
            DataFrame: The dataset in SparkNLP format.
        """
        if self._data_structure == DataStructure.SPARKNLP and isinstance(
            self._data, DataFrame
        ):
            return self._data
        else:
            self._load_data(data_structure=DataStructure.SPARKNLP)
            return self._data

    def load_data(self, data_structure: DataStructure) -> None:
        """Loads data into the dataset from the repository.

        Args:
            data_structure (DataStructure): The type of DataFrame to load (e.g., Pandas, Spark, SparkNLP).
        """
        try:
            self._repo = self._repo or DiscoverContainer.repo.dataset_repo()
            self._data = self._repo.read(
                filepath=self._filepath, data_structure=data_structure
            )
            self._data_structure = data_structure
        except Exception as e:
            raise (f"Unexpected error occurred. \n{e}")
