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
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Saturday January 4th 2025 04:36:50 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from dataclasses import field
from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.base.atype import AssetType
from discover.asset.base.identity import Passport
from discover.core.dstruct import DataClass
from discover.core.file import FileFormat
from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                       DATA SOURCE                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceDataConfig(DataClass):
    file_format: FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                     SOURCE DATASET                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceDatasetConfig(SourceDataConfig):
    phase: PhaseDef
    stage: StageDef


# ------------------------------------------------------------------------------------------------ #
#                                     SOURCE FILE                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class SourceFileConfig(SourceDataConfig):
    filepath: str


# ------------------------------------------------------------------------------------------------ #
#                                   DATASET PASSPORT                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetPassport(Passport):
    """
    Represents a passport assigned to each dataset created, encapsulating metadata and relationships.

    This class is used to manage metadata for datasets, including their asset type, source, parent dataset,
    and additional attributes inherited from the `Passport` base class.

    Attributes:
        asset_type (AssetType): The type of the asset, defaulting to `AssetType.DATASET`.
        source (Optional[DatasetPassport]): The dataset passport from which this dataset was derived.
        parent (Optional[DatasetPassport]): The parent dataset passport, representing hierarchical relationships.

    Methods:
        create: A factory method for creating instances of `DatasetPassport`.
    """

    asset_type: AssetType = AssetType.DATASET
    source: Optional[DatasetPassport] = field(default=None)
    parent: Optional[DatasetPassport] = field(default=None)

    def __post_init__(self) -> None:
        """Initializes the `DatasetPassport` with a default description if none is provided."""
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
        source: Optional[DatasetPassport] = field(default=None),
        parent: Optional[DatasetPassport] = field(default=None),
    ) -> DatasetPassport:
        """
        Factory method to create a new `DatasetPassport` instance.

        Args:
            asset_type (AssetType): The type of the asset, typically `AssetType.DATASET`.
            asset_id (str): A unique identifier for the dataset.
            phase (PhaseDef): The current phase of the dataset's lifecycle.
            stage (StageDef): The current stage of the dataset within its phase.
            name (str): The name of the dataset.
            created (datetime): The timestamp when the dataset was created.
            version (str): The version of the dataset.
            creator (Optional[str], optional): The creator of the dataset. Defaults to None.
            source (Optional[DatasetPassport], optional): The dataset passport from which this
                dataset was derived. Defaults to None.
            parent (Optional[DatasetPassport], optional): The parent dataset passport, representing
                hierarchical relationships. Defaults to None.

        Returns:
            DatasetPassport: A new instance of `DatasetPassport`.
        """
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
