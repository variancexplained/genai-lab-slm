#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/identity.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Friday December 27th 2024 05:36:43 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

from datetime import datetime

from pydantic.dataclasses import dataclass

from discover.asset.core import AssetType
from discover.core.data_structure import DataClass
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                     PASSPORT                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class Passport(DataClass):
    """
    Represents a basic passport containing metadata for an asset.

    The Passport class is a base class that tracks minimal metadata about an asset,
    such as its unique identifier, lifecycle phase, processing stage, and name.

    Attributes:

        asset_id (str): A unique identifier for the asset.
        asset_type (AssetType): Type of asset as specified in AssetType enumeration.
        phase (PhaseDef): The lifecycle phase of the asset (e.g., INGESTION, ANALYSIS).
        stage (StageDef): The processing stage of the asset (e.g., RAW, PROCESSED).
        name (str): A human-readable name for the asset.
        created (datetime): Datetime the asset was created.
    """

    asset_id: str
    asset_type: AssetType
    phase: PhaseDef
    stage: StageDef
    name: str
    created: datetime
