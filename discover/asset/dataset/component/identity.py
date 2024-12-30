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
# Modified   : Monday December 30th 2024 04:40:02 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.atype import AssetType
from discover.asset.base.identity import Passport
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                  DATASET PASSPORT                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetPassport(Passport):
    asset_type: AssetType = AssetType.DATASET
    source: Optional[DatasetPassport] = None
    parent: Optional[DatasetPassport] = None

    def __post_init__(self) -> None:
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
        description += f"in the {self.phase.label} - {self.stage.label} "
        description += f"on {self.created.strftime('%Y-%m-%d')} at {self.created.strftime('%H:%M:%S')}"
        return description

    @classmethod
    def create(
        cls,
        asset_type: AssetType,
        asset_id: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        created: datetime,
        version: str,
        creator: Optional[str] = None,
        source: Optional[DatasetPassport] = None,
        parent: Optional[DatasetPassport] = None,
    ) -> DatasetPassport:
        return cls(
            asset_type=asset_type,
            asset_id=asset_id,
            phase=phase,
            stage=stage,
            name=name,
            creator=creator,
            created=created,
            version=version,
            source=source,
            parent=parent,
        )
