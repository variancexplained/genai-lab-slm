#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/asset/dataset/dataset.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Saturday January 25th 2025 04:40:45 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Optional, Union

import pandas as pd
from genailabslm.analytics.dqa import DQA
from genailabslm.asset.base.asset import Asset
from genailabslm.asset.dataset.identity import DatasetPassport
from genailabslm.asset.dataset.state import DatasetState, DatasetStateDef
from genailabslm.infra.utils.file.fileset import FileSet
from pyspark.sql import DataFrame

# ------------------------------------------------------------------------------------------------ #
#                                       DATASET                                                    #
# ------------------------------------------------------------------------------------------------ #


class Dataset(Asset):
    """
    Represents a dataset in the asset repository.

    This class encapsulates the dataset's metadata (via `DatasetPassport`),
    its underlying data (as a DataFrame), its state, and event log. The dataset
    can be accessed, consumed, published, and have events logged, with tracking
    of its status throughout its lifecycle.

    Attributes:
        _passport (DatasetPassport): The passport containing the metadata for the dataset.
        _dataframe (Union[pd.DataFrame, DataFrame]): The actual data contained within the dataset.
        _state (DatasetState): The state of the dataset, tracking its current status.
        _file (Optional[FileSet]): The file set associated with the dataset, if available.
        _dqa (Optional[DQA]): The data quality analysis for the dataset.
        _accessed (Optional[datetime]): The timestamp when the dataset was last accessed.
        _created (datetime): The timestamp when the dataset was created.
        _status (DatasetState): The current status of the dataset.
        _eventlog (Dict[datetime, str]): A log of events associated with the dataset, with timestamps.

    Methods:
        __eq__(self, other: object) -> bool:
            Checks equality between two Dataset objects based on their asset ID and file path.

        __getstate__(self) -> dict:
            Prepares the object's state for serialization.

        __setstate__(self, state) -> None:
            Restores the object's state during deserialization.

        status(self) -> DatasetState:
            Returns the current status of the dataset.

        eventlog(self) -> pd.DataFrame:
            Returns the dataset's event log as a DataFrame.

        file(self) -> FileSet:
            Returns or sets the file set associated with the dataset.

        dqa(self) -> DQA:
            Returns the data quality analysis object for the dataset.

        dataframe(self) -> Union[pd.DataFrame, DataFrame]:
            Returns the underlying DataFrame object containing the dataset's data.

        serialize(self) -> pd.DataFrame:
            Prepares the dataset for serialization by nullifying the DataFrame.

        deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None:
            Restores the dataset's DataFrame after deserialization.

        get_registration(self) -> Dict[str, str]:
            Returns metadata and descriptive statistics for dataset registration.

        access(self, entity: Optional[str] = None) -> None:
            Marks the dataset as accessed by a specified entity.

        consume(self, entity: Optional[str] = None) -> None:
            Marks the dataset as consumed by a specified entity.

        publish(self, entity: Optional[str] = None) -> None:
            Marks the dataset as published to the repository by a specified entity.

        add_event(self, entity: str, event: str) -> None:
            Adds an event to the dataset's event log.

    Private Methods:

        _get_dqa(self) -> DQA:
            Returns the data quality analysis (DQA) for the dataset.
    """

    def __init__(
        self,
        passport: DatasetPassport,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
        state: DatasetState,
    ) -> None:
        super().__init__(passport=passport)
        self._passport = passport
        self._dataframe = dataframe
        self._state = state

        self._file = None
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
    def state(self) -> DatasetState:
        """Returns the DatasetState object for the Dataset"""
        return self._state

    @property
    def status(self) -> DatasetStateDef:
        """Returns the status of the Dataset"""
        return self._state.status

    @property
    def created(self) -> datetime:
        """Returns the status of the Dataset"""
        return self._state.created

    @property
    def published(self) -> datetime:
        """
        Returns the timestamp of when the dataset was published.

        Returns:
            datetime: The datetime the dataset was published.
        """
        return self._state.published

    @property
    def consumed(self) -> datetime:
        """
        Returns the timestamp of when the dataset was consumed.

        Returns:
            datetime: The datetime the dataset was consumed.
        """
        return self._state.consumed

    @property
    def eventlog(self) -> pd.DataFrame:
        """Returns the Datasets event log"""
        return self._state.get_events()

    @property
    def file(self) -> FileSet:
        """Returns the FileSet object for the Dataset"""
        return self._file

    @file.setter
    def file(self, file: FileSet) -> None:
        self._file = file

    @property
    def dqa(self) -> DQA:
        """Returns the Data Quality Analysis object."""
        self._dqa = self._dqa or self._get_dqa()
        return self._dqa

    @property
    def dataframe(self) -> Union[pd.DataFrame, DataFrame]:
        """Returns the underlying DataFrame object."""
        return self._dataframe

    def serialize(self) -> pd.DataFrame:
        """Prepare the object for serialization by nullifying the dataframe."""
        # Copy the dataframe.
        df = self._dataframe

        # Nullify the dataframe for serialization
        setattr(self, "_dataframe", None)

        return df

    def deserialize(self, dataframe: Union[pd.DataFrame, DataFrame]) -> None:
        """Deseriralizes the object

        Args:
            dataframe (Union[pd.DataFrame, DataFrame]): The DataFrame contents.
        """
        setattr(self, "_dataframe", dataframe)

    def get_registration(self) -> Dict[str, str]:
        """Returns a dictionary containing metadata and brief descriptive statistics for registration.

        Returns:
            Dict[str,str]: Dictionary containing metadata and descriptive statistics for the Dataset registry.
        """
        entry = self._passport.as_dict()
        entry.update(self._state.as_dict())
        return entry

    def access(self, entity: Optional[str] = None) -> None:
        """Method called by the repository when the dataset is accessed.

        Args:
            entity (str): The entity class name requesting access.
        """
        self._state.access(entity)

    def consume(self, entity: Optional[str] = None) -> None:
        """Method called when the Dataset has been consumed by a data processing or machine learning pipeline.

        Args:
            entity (str): The entity class name consuming the dataset.
        """
        self._state.consume(entity=entity)

    def publish(self, entity: Optional[str] = None) -> None:
        """Method called when the Dataset is being published to the repository.

        Args:
            entity (str): The entity class name publishing the dataset.
        """
        self._state.publish(entity=entity)

    def add_event(self, entity: str, event: str) -> None:
        """Adds an event to the event log

        Args:
            entity (str): The entity responsible for the event.
            event (str): A description of the event.
        """
        self._state.add_event(entity=entity, event=event)

    def _get_dqa(self) -> DQA:
        """Returns the Data Quality Analysis for the Dataset."""
        return DQA(dataset=self)
