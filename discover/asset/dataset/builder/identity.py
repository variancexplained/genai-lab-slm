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
# Modified   : Monday December 30th 2024 04:47:16 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Identity Builder Module"""
from __future__ import annotations

import inspect
from datetime import datetime
from typing import Optional

from dependency_injector.wiring import Provide, inject

from discover.asset.base.atype import AssetType
from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.dataset import Dataset
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
#                              DATASET PASSPORT BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #


class DatasetPassportBuilder(DatasetComponentBuilder):
    """
    Builder class for constructing DatasetPassport objects.

    Args:
        workspace (Workspace): The workspace instance to use for ID generation.
    """

    __ASSET_TYPE: AssetType = AssetType.DATASET

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
    ) -> None:
        self._workspace = workspace
        self._asset_type = self.__ASSET_TYPE
        self._asset_id: Optional[str] = None
        self._phase: Optional[PhaseDef] = None
        self._stage: Optional[StageDef] = None
        self._name: Optional[str] = None
        self._version: Optional[str] = None
        self._creator: Optional[str] = None
        self._created: Optional[datetime] = None
        self._source: Optional[Dataset] = None
        self._parent: Optional[DatasetPassport] = None
        self._dataset_passport: Optional[DatasetPassport] = None

    @property
    def passport(self) -> DatasetPassport:
        """
        Returns the constructed DatasetPassport and resets the builder.

        Returns:
            DatasetPassport: The constructed passport.

        Raises:
            ValueError: If `build()` was not called before accessing this property.
        """
        if not self._dataset_passport:
            raise ValueError("Passport has not been built. Call `build()` first.")
        passport = self._dataset_passport
        self.reset()
        return passport

    def reset(self) -> None:
        """Resets the builder to its initial state."""
        self._asset_id = None
        self._phase = None
        self._stage = None
        self._name = None
        self._creator = None
        self._created = None
        self._version = None
        self._source = None
        self._parent = None
        self._dataset_passport = None

    def phase(self, phase: PhaseDef) -> DatasetPassportBuilder:
        """Sets the phase for the dataset."""
        self._phase = phase
        return self

    def stage(self, stage: StageDef) -> DatasetPassportBuilder:
        """Sets the stage for the dataset."""
        self._stage = stage
        return self

    def name(self, name: str) -> DatasetPassportBuilder:
        """Sets the name for the dataset."""
        self._name = name
        return self

    def source(self, source: DatasetPassport) -> DatasetPassportBuilder:
        """Sets the source dataset."""
        self._source = source
        return self

    def parent(self, parent: DatasetPassport) -> DatasetPassportBuilder:
        """Sets the parent passport."""
        self._parent = parent
        return self

    def creator(self, creator: str) -> DatasetPassportBuilder:
        """Sets the creator of the passport."""
        self._creator = creator
        return self

    def build(self) -> DatasetPassportBuilder:
        """
        Builds the DatasetPassport after validating all fields.

        Returns:
            DatasetPassportBuilder: The builder instance for chaining.

        Raises:
            ValueError: If validation fails.
        """
        self._validate()
        self._created = datetime.now()
        self._asset_id, self._version = tuple(self._get_asset_id().as_dict().values())
        self._creator = self._creator or self._get_caller()
        self._dataset_passport = DatasetPassport.create(
            asset_type=self._asset_type,
            asset_id=self._asset_id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            creator=self._creator,
            created=self._created,
            version=self._version,
            source=self._source,
            parent=self._parent,
        )
        return self

    def _get_asset_id(self) -> str:
        """Generates a unique asset ID using the workspace."""
        return self._workspace.gen_asset_id(
            asset_type=self._asset_type,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
        )

    def _get_caller(self) -> str:
        # Get the stack frame of the caller
        stack = inspect.stack()
        caller_frame = stack[2]  # The caller of the caller of this method

        # Get the `self` object from the caller's local variables
        caller_self = caller_frame.frame.f_locals.get("self", None)
        # Get the class name if available.
        if caller_self:
            return caller_self.__class__.__name__
        else:
            return "User (not identified)"

    def _validate(self) -> None:
        """Validates that all required fields are set and properly typed."""
        errors = []
        if not isinstance(self._phase, PhaseDef):
            errors.append(f"Invalid phase: {self._phase} (Expected: PhaseDef)")
        if not isinstance(self._stage, StageDef):
            errors.append(f"Invalid stage: {self._stage} (Expected: StageDef)")
        if not isinstance(self._name, str):
            errors.append(f"Invalid name: {self._name} (Expected: str)")
        if errors:
            raise ValueError("\n".join(errors))
