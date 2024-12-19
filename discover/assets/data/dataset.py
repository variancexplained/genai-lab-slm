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
# Modified   : Thursday December 19th 2024 06:16:35 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.assets.base import Asset
from discover.core.data_structure import DataFrameType
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
        dataframe_type: Optional[DataFrameType] = None,
        creator: Optional[str] = None,
        parent: Optional[Dataset] = None,
    ) -> None:
        super().__init__(asset_id=asset_id, name=name, description=description)
        self._phase = phase
        self._stage = stage
        self._data = data
        self._creator = creator
        self._description = (
            self._description
            or f"Dataset asset {self._asset_id} - {self.name} created by {creator} on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        )
        self._persisted = None
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
    #                                       FLOW                                                    #
    # --------------------------------------------------------------------------------------------- #

    @property
    def phase(self) -> PhaseDef:
        """
        Returns the phase for which the asset was created.

        Returns:
            PhaseDef: Phase.
        """
        return self._phase

    @property
    def stage(self) -> StageDef:
        """
        Returns the stage for which the asset was created.

        Returns:
            StageDef: Stage.
        """
        return self._stage

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

        repo = DiscoverContainer.fileset_persistence.fileset_repo()

        self._data = repo.get(asset_id=self._asset_id, dataframe_type=dataframe_type)
        self._dataframe_type = dataframe_type

    def save_data(self) -> None:
        """Saves the dataset to the repository.

        Raises:
            RuntimeError: If no data is available to save.
        """

        if self._data is None:
            raise RuntimeError("No Data to save.")

        from discover.container import DiscoverContainer

        repo = DiscoverContainer.fileset_persistence.fileset_repo()

        self._filepath = repo.add(asset_id=self._asset_id, data=self._data)
        self._persisted = datetime.now()
