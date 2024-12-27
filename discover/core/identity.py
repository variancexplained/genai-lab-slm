#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/identity.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 04:15:12 am                                               #
# Modified   : Friday December 27th 2024 05:25:42 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from discover.core.asset import AssetType
from discover.core.data_structure import DataClass
from discover.core.flow import PhaseEnum, StageEnum


# ------------------------------------------------------------------------------------------------ #
@dataclass(frozen=True)
class Passport(DataClass):
    """
    Represents a basic passport containing metadata for an asset.

    The Passport class is a base class that tracks minimal metadata about an asset,
    such as its unique identifier, lifecycle phase, processing stage, and name.

    Attributes:
        asset_id (str): A unique identifier for the asset.
        phase (PhaseEnum): The lifecycle phase of the asset (e.g., INGESTION, ANALYSIS).
        stage (StageEnum): The processing stage of the asset (e.g., RAW, PROCESSED).
        name (str): A human-readable name for the asset.
    """

    asset_id: str
    phase: PhaseEnum
    stage: StageEnum
    name: str


# ------------------------------------------------------------------------------------------------ #


@dataclass(frozen=True)
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

    description: Optional[str] = None
    source: Optional[str] = None
    parent: Optional[str] = None
    asset_type: AssetType = AssetType.DATASET
    created: Optional[datetime] = None

    def __post_init__(self) -> None:
        """
        Performs post-initialization validation and default assignment.

        Ensures the `created` timestamp is set to the current time if not provided and generates
        a default description if none is supplied.
        """
        if self.created is None:
            self.created = datetime.now()

        if self.description is None:
            self.description = self._generate_default_description()

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
        description += f"in the {self.phase.description} - {self.stage.description} "
        description += f"on {self.created.strftime('%Y-%m-%d')} at {self.created.strftime('%H:%M:%S')}"
        return description
