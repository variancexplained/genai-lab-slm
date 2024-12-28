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
# Modified   : Friday December 27th 2024 08:17:54 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic.dataclasses import dataclass

from discover.asset.core import AssetType, Passport
from discover.asset.dataset.dataset import (
    Dataset,
    DatasetBuilder,
    DatasetComponentBuilder,
)
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


# ------------------------------------------------------------------------------------------------ #
#                              DATASET PASSPORT BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class DatasetPassportBuilder(DatasetComponentBuilder):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """

    __ASSET_TYPE: AssetType = AssetType.DATASET

    def __init__(self, dataset_builder: DatasetBuilder):
        self._dataset_builder = dataset_builder
        self._asset_type = self.__ASSET_TYPE
        self._asset_id = None
        self._phase = None
        self._stage = None
        self._name = None
        self._description = None
        self._created = None
        self._source = None
        self._parent = None

    def reset(self) -> None:
        self._asset_id = None
        self._phase = None
        self._stage = None
        self._name = None
        self._description = None
        self._created = None
        self._source = None
        self._parent = None

    def phase(self, phase: PhaseDef) -> DatasetPassportBuilder:
        self._phase = phase
        return self

    def stage(self, stage: StageDef) -> DatasetPassportBuilder:
        self._stage = stage
        return self

    def name(self, name: str) -> DatasetPassportBuilder:
        self._name = name
        return self

    def source(self, source: Dataset) -> DatasetPassportBuilder:
        self._source = source
        return self

    def build(self) -> DatasetPassportBuilder:
        self._validate_in()
        self._created = datetime.now()
        self._asset_id = self._get_asset_id()
        self._description = self._generate_default_description()
        self._dataset_passport = DatasetPassport.create(
            asset_type=self._asset_type,
            asset_id=self._asset_id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            created=self._created,
            description=self._description,
            source=self._source,
            parent=self._parent,
        )
        return self

    def _get_asset_id(self) -> None:
        workspace = self._dataset_builder.workspace
        return workspace.get_asset_id(
            asset_type=self._asset_type,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
        )

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

    def _validate_in(self) -> None:
        msg = ""
        msg += (
            f"Phase {self._phase} is invalid. Expected a PhaseDef type.\n"
            if not isinstance(self._phase, PhaseDef)
            else ""
        )
        msg += (
            f"Stage {self._stage} is invalid. Expected a StageDef type.\n"
            if not isinstance(self._stage, StageDef)
            else ""
        )

        msg += (
            f"Name {self._name} is invalid. Expected a string type.\n"
            if not isinstance(self._name, str)
            else ""
        )
