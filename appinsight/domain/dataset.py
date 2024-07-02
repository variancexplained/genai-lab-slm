#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/dataset.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:00:14 am                                                    #
# Modified   : Monday July 1st 2024 05:59:38 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd

from appinsight.domain.base import Entity
from appinsight.domain.config import DatasetConfig
from appinsight.domain.enums import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
class Dataset(Entity):
    """Encapsulates the Dataset entity."""

    def __init__(
        self,
        name: str,
        description: str,
        stage: str,
        phase: str = "PREP",
        content: pd.DataFrame = None,
        creator: str = None,
    ) -> None:
        super().__init__()
        self._oid = None
        self._name = name
        self._phase = phase
        self._stage = stage
        self._content = content
        self._creator = creator
        self._created = datetime.now()

        self._nrows = None
        self._ncols = None
        self._size = None
        self._cols = None

        if self._content is not None:
            self._populate_content_metadata(content=content)

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def oid(self) -> int:
        """Returns the unique identifier for the entity."""
        return self._oid

    @oid.setter
    def oid(self, oid: int) -> None:
        """Sets the object id."""
        self._oid = oid

    @property
    def content(self) -> pd.DataFrame:
        """Returns Dataset content"""
        return self._content

    @content.setter
    def content(self, content: pd.DataFrame) -> None:
        """Sets the dataset content"""
        self._content = content
        self._populate_content_metadata(content=content)

    @property
    def name(self) -> str:
        """Returns the name for the entity."""
        return self._name

    @property
    def phase(self) -> str:
        """Returns the phase in which the entity was created."""
        return self._phase

    @property
    def stage(self) -> str:
        """Returns the stage in which the entity was created."""
        return self._stage

    @property
    def size(self) -> str:
        """Returns the size of content in bytes."""
        return self._size

    @property
    def nrows(self) -> int:
        """Returns the number or rows in the dataset."""
        return self._nrows

    @property
    def ncols(self) -> int:
        """Returns the number or rows in the dataset."""
        return self._ncols

    @property
    def creator(self) -> str:
        """Class creating the dataset."""
        return self._creator

    @property
    def created(self) -> str:
        """Creation datetime."""
        return self._created

    @classmethod
    def from_config(cls, config: DatasetConfig) -> Dataset:
        """Instantiates a Dataset from a DatasetConfig object."""
        return cls(
            name=config.name,
            stage=config.stage,
            phase=config.phase,
            description=config.description,
        )

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> Dataset:
        """Instantiate a Dataset from a DataFrame."""
        df = df.iloc[0]
        return cls(
            oid=df["oid"],
            name=df["name"],
            description=df["description"],
            phase=df["phase"],
            stage=df["stage"],
            created=df["created"],
            creator=df["creator"],
            nrows=df["nrows"],
            ncols=df["ncols"],
            size=df["size"],
        )

    def to_df(self) -> pd.DataFrame:
        """Returns the object variables in a DataFrame."""
        self.validate()
        d = {
            "name": self._name,
            "description": self._description,
            "phase": self._phase,
            "stage": self._stage,
            "created": self._created,
            "creator": self._creator,
            "nrows": self._nrows,
            "ncols": self._ncols,
            "size": self._size,
        }
        return pd.DataFrame(data=d, index=[0])

    def _populate_content_metadata(self, content: pd.DataFrame) -> None:
        """Sets content related variables."""
        self._nrows = len(content)
        self._ncols = content.shape[0]
        self._size = content.memory_usage(deep=True)
        self._cols = content.columns

    def validate(self) -> None:
        """Validates construction parameters."""
        # Validate content
        if not isinstance(self._content, pd.DataFrame):
            msg = "Content is not a valid DataFrame object."
            self._logger.exception(msg)
            raise ValueError(msg)

        # Verify dataframe is not empty
        if len(self._content) == 0:
            msg = "Empty DataFrame"
            self._logger.exception(msg)
            raise ValueError(msg)

        # Validate stage
        if self._stage not in Stage.__members__.keys():
            msg = f"Stage {self._stage} is invalid."
            self._logger.exception(msg)
            raise ValueError(msg)

        # Validate phase
        if self._phase not in Phase.__members__.keys():
            msg = f"Phase {self._phase} is invalid."
            self._logger.exception(msg)
            raise ValueError(msg)

        # Validate creator
        if self._creator is None:
            msg = "Creator is None"
            self._logger.exception(msg)
            raise TypeError(msg)
