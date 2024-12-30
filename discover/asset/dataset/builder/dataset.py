#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder/dataset.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Sunday December 29th 2024 05:27:36 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

import logging

from dependency_injector.wiring import Provide, inject

from discover.asset.base.builder import AssetBuilder
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.dataset import Dataset
from discover.container import DiscoverContainer
from discover.infra.exception.dataset import DatasetBuilderError
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                               DATASET BUILDER                                                    #
# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(AssetBuilder):

    @inject
    def __init__(
        self,
        workspace: Workspace = Provide[DiscoverContainer.workspace.service],
    ) -> None:
        self._workspace = workspace
        self._passport = None
        self._data = None

        self._dataset = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def dataset(self) -> Dataset:
        """
        Retrieves the constructed `Dataset` object and resets the builder's state.

        Returns:
            Dataset: The constructed dataset object.
        """
        dataset = self._dataset
        self.reset()
        return dataset

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all configured components, preparing the builder for a new configuration.
        """
        self._passport = None
        self._data = None
        self._dataset = None

    def passport(self, passport: DatasetPassport) -> DatasetBuilder:
        self._passport = passport
        return self

    def data(self, data: DataComponent) -> DatasetBuilder:
        self._data = data
        return self

    def build(self) -> DatasetBuilder:
        """
        Constructs the final `Dataset` object based on the provided configurations.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._validate()
        dataset = Dataset(
            passport=self._passport,
            workspace=self._workspace,
            data=self._data,
        )
        self._workspace.dataset_repo.add(asset=dataset)
        self._dataset = dataset
        return self

    def _validate(self) -> None:
        """
        Validates the builder's current state to ensure all required components
        are set.

        Raises:
            DatasetBuilderError: If any required components are missing.
        """
        msg = ""
        msg += "Dataset passport not set\n" if self._passport is None else ""
        msg += "Data component not set\n" if self._data is None else ""
        if msg:
            raise DatasetBuilderError(msg)

    def _check_passport(self) -> None:
        """
        Ensures that the passport has been constructed before building other components.

        Raises:
            RuntimeError: If the passport has not been constructed.
        """
        if self._passport is None:
            raise RuntimeError(
                "The passport must be constructed before building the data component."
            )
