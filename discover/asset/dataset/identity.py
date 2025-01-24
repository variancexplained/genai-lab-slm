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
# Modified   : Friday January 24th 2025 09:13:57 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.identity import AssetPassport


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET PASSPORT                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetPassport(AssetPassport):
    """Encapsulates dataset identity metadata.

    This class provides a structured way to represent the identity and
    provenance of a dataset. It inherits the following from the Passport base class:

    Inherits from `AssetPassport`

    Args:
        source (DatasetPassport): The source dataset passport.
    """

    source: Optional[DatasetPassport] = field(default=None)

    def __post_init__(self) -> None:
        """Sets the description if not already set."""
        self.created = datetime.now()
        self.description = self.description or self._generate_default_description()

    def _generate_default_description(self) -> str:
        """
        Generates a default description for the dataset based on its attributes.

        The description includes the dataset's name, source (if available), phase, stage, and
        creation timestamp.

        Returns:
            str: A default description for the dataset.
        """
        description = f"Dataset {self.name} created "
        if self.source:
            description += f"from {self.source.asset_id} "
        description += (
            f"in the {self.phase.label} - {self.stage.label} by {self.creator}."
        )
        return description
