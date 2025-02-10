#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/base/identity.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 21st 2025 03:21:59 am                                               #
# Modified   : Sunday February 9th 2025 12:22:46 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Workspace Identity Base Module"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from genailab.core.dstruct import DataClass
from genailab.core.flow import PhaseDef, StageDef
from genailab.infra.utils.file.fileset import FileFormat


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
        phase (PhaseDef): The lifecycle phase of the asset (e.g., PREPROCESSION, ANALYSIS).
        stage (StageDef): The processing stage of the asset (e.g., RAW, PROCESSED).
        name (str): A human-readable name for the asset.
        description (Optional[str]): Description for the asset.
        file_format (FileFormat): The file format of the asset.
        creator (Optional[str]): The name of the class that created the asset. Optional.
        created (datetime): Datetime the asset was created.
        version (str): Version of the asset.
    """

    asset_id: Optional[str] = field(default=None)
    phase: Optional["PhaseDef"] = field(default=None)
    stage: Optional["StageDef"] = field(default=None)
    name: Optional[str] = field(default=None)
    description: Optional[str] = field(default=None)
    file_format: "FileFormat" = FileFormat.PARQUET
    creator: Optional[str] = field(default=None)
    created: Optional[datetime] = None

    def __post_init__(self) -> None:
        self.created = datetime.now()
