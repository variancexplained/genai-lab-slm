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
# Created    : Tuesday January 21st 2025 03:21:59 am                                               #
# Modified   : Wednesday January 22nd 2025 01:42:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Workspace Identity Base Module"""
from __future__ import annotations

from dataclasses import field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic.dataclasses import dataclass

from discover.core.dstruct import DataClass
from discover.core.dtypes import IMMUTABLE_TYPES, SEQUENCE_TYPES
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                         PASSPORT                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class AssetPassport(DataClass):
    """
    Represents a basic passport containing identity metadata for an asset.

    The Passport class is a base class that tracks minimal metadata about an asset,
    such as its unique identifier, lifecycle phase, processing stage, name, and description.

    Attributes:

        asset_id (str): A unique identifier for the asset.
        phase (PhaseDef): The lifecycle phase of the asset (e.g., INGESTION, ANALYSIS).
        stage (StageDef): The processing stage of the asset (e.g., RAW, PROCESSED).
        name (str): A human-readable name for the asset.
        description (Optional[str]): Description for the asset.
        file_format (FileFormat): The file format of the asset.
        creator (Optional[str]): The name of the class that created the asset. Optional.
        created (datetime): Datetime the asset was created.
        version (str): Version of the asset.
    """

    asset_id: Optional[str] = field(default=None)
    phase: Optional[PhaseDef] = field(default=None)
    stage: Optional[StageDef] = field(default=None)
    name: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)
    file_format: FileFormat = FileFormat.PARQUET
    creator: Optional[str] = field(default=None)
    created: Optional[datetime] = field(default=None)
    accessed: Optional[datetime] = field(default=None)
    version: Optional[str] = field(default=None)

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
        elif isinstance(v, AssetPassport):
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
