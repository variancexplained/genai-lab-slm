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
# Modified   : Tuesday July 2nd 2024 05:12:56 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Entity Module"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from appinsight.domain.base import Entity
from appinsight.domain.enums import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
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
        self.size = self.content.memory_usage(deep=True).sum().sum()
        self.cols = list(self.content.columns)
        self.created = self.created or datetime.now()

    def validate(self) -> None:
        """Validates construction parameters."""

        self._validate_name()
        self._validate_content()
        self._validate_stage()
        self._validate_phase()
        self._validate_creator()
        self._validate_created()

    def _validate_name(self) -> None:
        """Validates name is at least 3 characters long."""
        valid = True
        if self.name is None:
            valid = False
        elif len(self.name) - self.name.count(" ") < 3:
            valid = False

        if valid is False:
            msg = "The name member must have at least 3 non-space characters."
            print(msg)
            raise ValueError(msg)

    def _validate_content(self) -> None:
        """Ensures content is a non-empty pandas DataFrame."""
        if not isinstance(self.content, pd.DataFrame):
            msg = "Content is not a valid DataFrame object."
            print(msg)
            raise TypeError(msg)

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

    def _validate_created(self) -> None:
        """Ensures created is a datetime object"""
        if not isinstance(self.created, datetime):
            msg = "Created must be a datetime object." ""
            print(msg)
            raise TypeError(msg)
