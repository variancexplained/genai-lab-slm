#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/component/identity.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Friday December 27th 2024 10:03:15 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.core import AssetType
from discover.asset.identity import Passport
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET PASSPORT                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass()
class DatasetPassport(Passport):
    """
    Extends the Passport class to include dataset-specific metadata and lifecycle attributes.

    The DatasetPassport class provides additional fields for datasets, such as description,
    source, parent asset reference, asset type, and creation timestamp. It automatically
    generates a default description if none is provided.

    Attributes:
        description (Optional[str]): A textual description of the dataset. Defaults to None,
            in which case a default description is generated.
        source (Optional[str]): The source of the dataset (e.g., file path, URL). Defaults to None.
        parent (Optional[str]): A reference to the parent asset's ID, if applicable. Defaults to None.
        asset_type (AssetType): The type of the asset. Defaults to `AssetType.DATASET`.
        created (Optional[datetime]): The timestamp when the dataset was created. Defaults to the
            current time if not provided.

    Methods:
        _generate_default_description(): Generates a default description for the dataset
            based on its attributes.

    Raises:
        ValueError: If any required attributes fail validation during initialization.
    """

    asset_type: AssetType = AssetType.DATASET
    description: Optional[str] = None
    source: Optional[str] = None
    parent: Optional[str] = None

    @classmethod
    def create(
        cls,
        asset_type: AssetType,
        asset_id: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        created: datetime,
        description: str = None,
        source: str = None,
        parent: str = None,
    ) -> DatasetPassport:
        return cls(
            asset_type=asset_type,
            asset_id=asset_id,
            phase=phase,
            stage=stage,
            name=name,
            description=description,
            source=source,
            parent=parent,
            created=created,
        )
