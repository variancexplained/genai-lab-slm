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
# Modified   : Saturday December 28th 2024 01:08:18 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

import logging
from typing import Type

from discover.asset.base import AssetBuilder
from discover.asset.dataset.builder.data import DataComponentBuilder
from discover.asset.dataset.builder.identity import DatasetPassportBuilder
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.component.ops import DatasetOps
from discover.asset.dataset.dataset import Dataset
from discover.infra.exception.dataset import DatasetBuilderError
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                               DATASET BUILDER                                                    #
# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(AssetBuilder):
    """
    Abstract base class for building datasets with configurable phases, stages,
    and formats. The builder pattern supports ingesting data from multiple sources,
    configuring internal representations, and exporting to various formats.
    """

    def __init__(
        self,
        workspace: Workspace,
        ops: DatasetOps,
        passport_builder_cls: Type[DatasetPassportBuilder] = DatasetPassportBuilder,
        source_builder_cls: Type[DataComponentBuilder] = DataComponentBuilder,
    ) -> None:
        # Internal resources
        self._workspace = workspace

        # Component Builders
        self._passport_builder = passport_builder_cls(workspace=self._workspace)
        self._source_builder_cls = source_builder_cls

        # Dataset Components
        self._passport = None
        self._data = None
        self._ops = ops
        self._repo = self._workspace.dataset_repo

        self._dataset = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def dataset(self) -> Dataset:
        dataset = self._dataset
        self.reset()
        return dataset

    def reset(self) -> None:
        self._passport = None
        self._data = None
        self._dataset = None

    @property
    def passport(self) -> DatasetPassportBuilder:
        return self._passport_builder

    @property
    def source(self) -> DataComponentBuilder:
        if self._passport is None:
            raise RuntimeError(
                "The passport must be constructed before the data source."
            )
        return self._source_builder_cls(passport=self._passport)

    def build(self) -> DatasetBuilder:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """

        # Construct the passport
        self._passport = self._build_passport()

        # Get data component
        data = self._source_builder.data

        # Construct the Dataset
        dataset = Dataset(
            passport=self._passport,
            workspace=self._workspace,
            data=self.source.data,
            ops=self._ops,
        )

        # Validate Dataset
        self._validate()

        # Register Dataset
        self._workspace.dataset_repo.add(asset=dataset)

        # Present constructed dataset as property
        self._dataset = dataset

    def _build_dataset(
        self, passport: DatasetPassport, data_envelope: DataComponent
    ) -> None:
        """Constructs the Dataset object"""
        return

    def _register_dataset(self, dataset: Dataset) -> Dataset:
        """Registers the dataset with an asset_id, filepath, then persists it."""
        # Assign an asset_id
        dataset = self._workspace_service.set_asset_id(asset=dataset)
        # Set the filepath in the workspace
        dataset = self._workspace_service.set_filepath(asset=dataset)
        # Persist the Dataset in the workspace
        self._workspace_service.dataset_repo.add(asset=dataset)

        return dataset

    def _validate(self, dataset: Dataset) -> None:
        self._validate_passport(dataset=dataset)
        self._validate_workspace(dataset=dataset)
        self._validate_data_envelope(dataset=dataset)
        self._validate_ops(dataset=dataset)

    def _validate_passport(self, dataset: Dataset) -> None:
        msg = ""
        msg += "Dataset passport not set\n" if self._dataset.passport is None else ""
        msg += (
            "Dataset data_envelope not set\n"
            if self._dataset.data_envelope is None
            else ""
        )
        msg += "Dataset operations not set\n" if self._dataset.data_ops is None else ""
        if msg:
            raise DatasetBuilderError(msg)
