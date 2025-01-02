#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/base/identity.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Thursday January 2nd 2025 06:46:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.atype import AssetType
from discover.core.dstruct import DataClass
from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                     ASSET ID                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AssetId(DataClass):
    name: str
    version: str


# ------------------------------------------------------------------------------------------------ #
#                                     PASSPORT                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
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
        version (str): Version of the asset.
    """

    asset_id: Optional[str] = None
    asset_type: Optional[AssetType] = None
    phase: Optional[PhaseDef] = None
    stage: Optional[StageDef] = None
    name: Optional[str] = None
    description: Optional[str] = None
    creator: Optional[str] = None
    created: Optional[datetime] = None
    version: Optional[str] = None

    @classmethod
    def _export_config(
        cls,
        v: Any,
    ) -> Any:
        """Returns v with Configs converted to dicts, recursively."""
        if isinstance(v, IMMUTABLE_TYPES):
            return v
        elif isinstance(v, SEQUENCE_TYPES):
            return type(v)(map(cls._export_config, v))
        elif isinstance(v, Passport):
            return v.asset_id
        elif isinstance(v, dict):
            return v
        elif hasattr(v, "as_dict"):
            return v.as_dict()
        elif isinstance(v, datetime):
            return v.isoformat()
        elif isinstance(v, (PhaseDef, StageDef)):
            return v.label
        elif isinstance(v, Enum):
            if hasattr(v, "label"):
                return v.label
            else:
                return v.value
        else:
            return dict()
