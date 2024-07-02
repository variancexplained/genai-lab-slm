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
# Modified   : Tuesday July 2nd 2024 11:55:51 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
from __future__ import annotations

from dataclasses import field
from datetime import datetime

import pandas as pd

from appinsight.domain.base import Entity
from appinsight.domain.enums import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
class Dataset(Entity):
    """Encapsulates the Dataset entity."""

    content: pd.DataFrame = None
    nrows: int = None
    ncols: int = None
    size: int = None
    cols: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.validate()
        self.nrows = len(self.content)
        self.ncols = self.content.shape[0]
        self.size = self.content.memory_usage(deep=True)
        self.cols = self.content.columns
        self.created = self.created or datetime.now()

    @classmethod
    def from_repo(cls, df: pd.DataFrame, content: pd.DataFrame) -> Dataset:
        """Instantiate a Dataset from a DataFrame."""
        return cls(
            oid=df["oid"],
            name=df["name"],
            description=df["description"],
            phase=df["phase"],
            stage=df["stage"],
            creator=df["creator"],
            created=df["created"],
            content=content,
            nrows=df["nrows"],
            ncols=df["ncols"],
            size=df["size"],
            cols=content.columns,
        )

    def validate(self) -> None:
        """Validates construction parameters."""

        self._validate_content()
        self._validate_stage()
        self._validate_phase()
        self._validate_creator()

    def _validate_content(self) -> None:
        """Ensures content is a non-empty pandas DataFrame."""
        if not isinstance(self.content, pd.DataFrame):
            msg = "Content is not a valid DataFrame object."
            print(msg)
            raise ValueError(msg)

        # Verify dataframe is not empty
        if len(self.content) == 0:
            msg = "Empty DataFrame"
            print(msg)
            raise ValueError(msg)

    def _validate_stage(self) -> None:
        """Ensures stage is valid."""
        if self.stage not in Stage.__members__.keys():
            msg = f"Stage {self.stage} is invalid."
            print(msg)
            raise ValueError(msg)

    def _validate_phase(self) -> None:
        """Ensures phase is valid"""
        if self.phase not in Phase.__members__.keys():
            msg = f"Phase {self.phase} is invalid."
            print(msg)
            raise ValueError(msg)

    def _validate_creator(self) -> None:
        """Ensures creator is non-empty"""
        if self.creator is None:
            msg = "Creator is None"
            print(msg)
            raise ValueError(msg)
