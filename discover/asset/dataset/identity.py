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
# Modified   : Tuesday January 21st 2025 06:28:39 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.identity import AssetPassport
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

    source: Optional[DatasetPassport] = field(default=None)
    dftype: Optional[DFType] = DFType.SPARK
    filepath: Optional[str] = field(default=None)

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
