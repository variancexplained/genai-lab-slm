#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/dataset/dataset.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Monday February 3rd 2025 06:01:19 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional, Type, Union

import pandas as pd
from pyspark.sql import DataFrame

from genailab.analytics.dqa import DQA
from genailab.analytics.eda import EDA
from genailab.asset.base.asset import Asset
from genailab.asset.dataset.identity import DatasetPassport
from genailab.asset.dataset.state import DatasetState, DatasetStateDef
from genailab.infra.utils.file.fileset import FileSet
from genailab.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
#                                       DATASET                                                    #
# ------------------------------------------------------------------------------------------------ #
if TYPE_CHECKING:
    from genailab.asset.dataset.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #

class Dataset(Asset):

    def __init__(
        self,
        passport: DatasetPassport,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
        state: DatasetState,
        repo: DatasetRepo,
        eda_cls: Type[EDA] = EDA,
    ) -> None:
        super().__init__(passport=passport)
        self._passport = passport
        self._dataframe = dataframe
        self._state = state
        self._repo = repo
        self._eda_cls = eda_cls
        self._eda = None

        self._file = None
        self._dqa = None
        self._profile = None
        self._summary = None

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
    def profile(self) -> pd.DataFrame:
        """Returns a profile of the data, including data types, null values, and uniqueness."""
        if self._profile is None:
            self._set_profile()
        return self._profile

    @property
    def summary(self) -> None:
        """Prints a Dataset summary."""
        if self._summary is None:
            self._set_summary()
        title = f"AppVoCAI Dataset Summary\n{self._passport.phase.label}\n{self._passport.stage.label}"
        Printer().print_dict(title=title, data=self._summary)

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

    def _set_eda(self) -> EDA:
        """Returns the EDA object for Dataset analysis."""
        if isinstance(self._dataframe, DataFrame):
            from genailab.infra.service.data.convert import Converter
            df = Converter.to_pandas(df=self._dataframe)
            self._eda = self._eda_cls(df=df)
        else:
            self._eda = self._eda_cls(df=self._dataframe)

    def _set_profile(self) -> None:
        """Sets the Dataset profile object."""
        if not isinstance(self._eda, EDA):
            self._set_eda()
        self._profile = self._eda.profile

    def _set_summary(self) -> None:
        """Sets the Dataset summary object."""
        if not isinstance(self._eda, EDA):
            self._set_eda()
        self._summary = self._eda.summarize()
