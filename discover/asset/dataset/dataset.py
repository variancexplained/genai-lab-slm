#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/dataset.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Wednesday January 22nd 2025 02:57:10 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from typing import Dict, Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.analytics.dqa import DQA
from discover.asset.base.asset import Asset
from discover.asset.dataset.dataframer import DataFramer
from discover.asset.dataset.identity import DatasetPassport
from discover.infra.utils.file.fileset import FileAttr, FileSet

# ------------------------------------------------------------------------------------------------ #
#                                   DATA COMPONENT                                                 #
# ------------------------------------------------------------------------------------------------ #


class Dataset(Asset):
    """A Dataset object represents a reviews dataset, its identifying properties, and metadata.

    This class encapsulates the data files, metadata (passport), and a data manipulation object (DataFramer) associated with a dataset.

    Args:
        passport (DatasetPassport): Key identifying information and metadata such as the dataset asset_id,
            phase, stage, name, description, file format, and dataframe type.
        dataframer (DataFramer): Wrapper for the DataFrame, encapsulating its data as well as
            metadata and basic descriptive statistics.

    Attributes:
        _ passport (DatasetPassport): The dataset's passport containing metadata.
        _ file (FileSet): File metadata
        _ dataframer (DataFramer): Object encapsulating the DataFrame and select metadata and basic summary statistics.
        _logger (logging.Logger): A logger object for recording dataset events.
        _dqa (DQA, optional): The Data Quality Analysis object associated with the dataset, if it has undergone data quality assessment.

    Methods:
        __eq__(self, other) -> bool: Compares two Dataset objects for equality.
        __getstate__(self) -> dict: Prepares the object's state for serialization.
        __setstate__(self, state) -> None: Restores the object's state during deserialization.
        @property
        def file(self) -> FileSet: Provides access to the FileSet object.
        @property
        def info(self) -> pd.DataFrame: Returns the data files' information.
        @property
        def summary(self) -> None: Prints a summary of the data files.
        @property
        def dqa(self) -> DQA: Gets the Data Quality Analysis object, if it exists. Raises an error if not available.
        @property
        def dataframe(self) -> Union[pd.DataFrame, DataFrame]: Gets the underlying pandas DataFrame from the DataFramer object.
        def serialize(self) -> pd.DataFrame: Prepares the object for serialization by nullifying the dataframe.
        def deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None: Restores the dataframe during deserialization.
    """

    def __init__(
        self,
        passport: DatasetPassport,
        dataframer: DataFramer,
    ) -> None:
        super().__init__(passport=passport)
        self._passport = passport
        self._dataframer = dataframer

        self._file = None
        self._info = None
        self._summary = None
        self._dqa = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other: object) -> bool:
        """Checks equality between two Asset objects based on their asset ID."""
        if isinstance(other, Dataset):
            if self._passport == other.passport and self.file.path == other.file.path:
                return True
            else:
                return False
        else:
            return False

    def __getstate__(self) -> dict:
        """Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        return self.__dict__.copy()

    def __setstate__(self, state) -> None:
        """Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)

    @property
    def file(self) -> FileSet:
        """Returns the FileSet object for the Dataset"""
        self._file = self._file or self._get_fileset()
        return self._file

    @property
    def info(self) -> pd.DataFrame:
        """Returns a quantitative summary of the DataFrame structure and properties."""
        self._info = self._info or self._get_info()
        return self._info

    @property
    def summary(self) -> Dict[str, str]:
        """Prints descriptive and summary statistics on DataFrame columns and observations."""
        self._summary = self._summary or self._get_summary()

    @property
    def dqa(self) -> DQA:
        """Returns the Data Quality Analysis object."""
        self._dqa = self._dqa or self._get_dqa()
        return self._dqa

    @property
    def dataframe(self) -> Union[pd.DataFrame, DataFrame]:
        """Returns the underlying DataFrame object."""
        return self._dataframer.dataframe

    def serialize(self) -> pd.DataFrame:
        """Prepare the object for serialization by nullifying the dataframe."""
        # Copy the dataframe.
        df = self._dataframer.dataframe

        # Nullify the dataframe for serialization
        self._dataframer.dataframe = None

        return df

    def deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None:
        """Deseriralizes the object

        Args:
            dataframe (Union[pd.DataFrame, DataFrame]): The DataFrame contents.
        """
        self._dataframer.dataframe = dataframe

    def register(self) -> Dict[str, str]:
        """Returns a dictionary containing metadata and brief descriptive statistics for registration.

        Returns:
            Dict[str,str]: Dictionary containing metadata and descriptive statistics for the Dataset registry.
        """
        passport = self._passport.as_dict()
        passport.update(self.summary)
        return passport

    def access(self) -> None:
        """Upddates the Dataset datetime last accessed datetime."""
        self._passport.access()

    def publish(self) -> None:
        """Adds a publish event to the Dataset event log and changes the state to `PUBLISHED`."""
        self._passport.publish()

    def consume(self) -> None:
        """Updates the Dataset state to `CONSUMED` and adds an event to the event log."""
        self._passport.consume()

    def remove(self) -> None:
        """Updates the Dataset status to `REMOVED`."""
        self._passport.remove()

    def _get_fileset(self) -> FileSet:
        """Returns a FileSet object containing the Dataset's file metadata."""
        return FileAttr.get_fileset(
            filepath=self._passport.filepath, file_format=self._passport.file_format
        )

    def _get_info(self) -> Union[pd.DataFrame, DataFrame]:
        """Returns a DataFrame containing DataFrame structural information"""
        self._dataframer.info()

    def _get_summary(self) -> None:
        """Prints a qualitative and descriptive summary of the Dataset."""
        self._dataframer.summary()

    def _get_dqa(self) -> DQA:
        """Returns the Data Quality Analysis for the Dataset."""
        return DQA(dataset=self)
