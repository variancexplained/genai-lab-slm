#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/dataset/identity.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:42:37 am                                               #
# Modified   : Sunday February 9th 2025 12:23:52 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from genailab.asset.base.identity import AssetPassport
from genailab.core.dtypes import DFType


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
        source (str): The source dataset asset_id.
    """

    source: Optional[str] = field(default=None)
    dftype: Optional[DFType] = field(default=None)

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
            description += f"from {self.source} "
        description += (
            f"in the {self.phase.label} - {self.stage.label} by {self.creator}."
        )
        return description
