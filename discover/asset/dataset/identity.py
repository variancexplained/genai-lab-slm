#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/identity.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:42:37 am                                               #
# Modified   : Wednesday January 22nd 2025 01:45:02 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Dict, Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.identity import AssetPassport
from discover.asset.dataset.state import DatasetState
from discover.core.dtypes import DFType


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET PASSPORT                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetPassport(AssetPassport):
    """Encapsulates dataset identity metadata.

    This class provides a structured way to represent the identity and
    provenance of a dataset. It inherits the following from the Passport base class:

    Inherits from `AssetPassport`

    Attributes:
        source (Optional[DatasetPassport]): A reference to the source dataset.
        dftype (Optional[DFType]): The DataFrame type. Defaults to `DFType.SPARK`.
        filepath (str): The location of the data file.

    """

    status: Optional[DatasetState] = DatasetState.CREATED
    source: Optional[DatasetPassport] = field(default=None)
    dftype: Optional[DFType] = DFType.SPARK
    filepath: Optional[str] = field(default=None)
    eventlog: Dict[datetime, str] = field(
        default_factory=lambda: ({datetime.now(): "Dataset created."})
    )

    def __post_init__(self) -> None:
        """Sets the description if not already set."""
        self.description = self.description or self._generate_default_description()

    def _generate_default_description(self) -> str:
        """
        Generates a default description for the dataset based on its attributes.

        The description includes the dataset's name, source (if available), phase, stage, and
        creation timestamp.

        Returns:
            str: A default description for the dataset.
        """
        self.description = f"Dataset {self.name} created "
        if self.source:
            self.description += f"from {self.source.asset_id} "
        self.description += f"in the {self.phase.label} - {self.stage.label} "
        self.description += f"on {self.created.strftime('%Y-%m-%d')} at {self.created.strftime('%H:%M:%S')}"

    def access(self) -> None:
        """Method called by the repository when the dataset is accessed."""
        dt = datetime.now()
        self.eventlog[dt] = "Dataset Accessed"
        self.accessed = dt

    def consume(self) -> None:
        """Method called when the Dataset has been consumed by a data processing or machine learning pipeline."""
        self.status = DatasetState.CONSUMED
        self.eventlog[datetime.now()] = "Dataset Consumed"

    def publish(self) -> None:
        """Method called when the Dataset is being published to the repository."""
        self.status = DatasetState.PUBLISHED
        self.eventlog[datetime.now()] = "Dataset Published"

    def remove(self) -> None:
        """Method called when the Dataset is being removed from the repository"""
        self.status = DatasetState.REMOVED
        self.eventlog[datetime.now()] = "Dataset Removed"
