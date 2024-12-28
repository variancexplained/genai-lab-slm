#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder/identity.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Saturday December 28th 2024 10:52:17 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Identity Builder Module"""
from __future__ import annotations

from datetime import datetime

from discover.asset.core import AssetType
from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import PhaseDef, StageDef
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                              DATASET PASSPORT BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class DatasetPassportBuilder(DatasetComponentBuilder):
    """
    Abstract base class for building assets with phases, stages, and persistence
    configurations.
    """

    __ASSET_TYPE: AssetType = AssetType.DATASET

    def __init__(self, workspace: Workspace):
        self._workspace = workspace
        self._asset_type = self.__ASSET_TYPE
        self._asset_id = None
        self._phase = None
        self._stage = None
        self._name = None
        self._description = None
        self._created = None
        self._source = None
        self._parent = None

    @property
    def passport(self) -> DatasetPassport:
        passport = self._dataset_passport
        self.reset()
        return passport

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
        self._validate()
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
        return self._workspace.get_asset_id(
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
        description = f"Dataset {self._name} created "
        if self.source:
            description += f"from {self._source} "
        description += f"in the {self._phase.description} - {self._stage.description} "
        description += f"on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('%H:%M:%S')}"
        return description

    def _validate(self) -> None:
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
        if msg:
            raise ValueError(msg)
