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
# Modified   : Saturday December 28th 2024 02:39:52 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

import logging
from typing import Type

from discover.asset.base import AssetBuilder
from discover.asset.dataset.builder.data import (
    DataComponentBuilder,
    DFSourceDataComponentBuilder,
    FileSourceDataComponentBuilder,
)
from discover.asset.dataset.builder.identity import DatasetPassportBuilder
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
    A builder class for constructing a `Dataset` object.

    This builder provides a fluent interface for configuring and constructing a
    `Dataset` that encapsulates its passport, data components, and operations.
    It manages sub-builders for constructing dataset components such as the passport
    and data component.

    Args:
        workspace (Workspace): The workspace that manages datasets, their storage,
            and associated metadata.
        ops (DatasetOps): The operations available for the dataset (e.g., conversion,
            splitting, merging).
        passport_builder_cls (Type[DatasetPassportBuilder]): The class to use for
            building the dataset passport. Defaults to `DatasetPassportBuilder`.
        data_component_builder_cls (Type[DataComponentBuilder]): The class to use for
            building the data component. Defaults to `DataComponentBuilder`.

    Attributes:
        dataset (Dataset): The constructed `Dataset` object. Accessing this property
            resets the builder's state.

    Methods:
        reset() -> None:
            Resets the builder's internal state.

        passport() -> DatasetPassport:
            Returns the constructed passport for the dataset.

        with_passport() -> DatasetPassportBuilder:
            Initializes and returns the passport builder for configuring the dataset's
            passport.

        from_dataframe() -> DataComponentBuilder:
            Initializes and returns the data component builder for constructing the
            dataset from an in-memory dataframe.

        from_file() -> DataComponentBuilder:
            Initializes and returns the data component builder for constructing the
            dataset from a file.

        build() -> DatasetBuilder:
            Constructs the final `Dataset` object based on the configured components.

    Internal Methods:
        _validate() -> None:
            Validates the builder's current state to ensure all required components
            are set.

        _check_passport() -> None:
            Ensures that the passport has been constructed before building other components.

    Raises:
        DatasetBuilderError: If validation fails during the build process.
        RuntimeError: If a required component, such as the passport, is not set before
            building subsequent components.
    """

    def __init__(
        self,
        workspace: Workspace,
        ops: DatasetOps,
        passport_builder_cls: Type[DatasetPassportBuilder] = DatasetPassportBuilder,
        df_data_component_builder_cls: Type[
            DFSourceDataComponentBuilder
        ] = DFSourceDataComponentBuilder,
        file_data_component_builder_cls: Type[
            FileSourceDataComponentBuilder
        ] = FileSourceDataComponentBuilder,
    ) -> None:
        # Resources
        self._workspace = workspace
        self._ops = ops

        # Builders
        self._passport_builder = passport_builder_cls(workspace=self._workspace)
        self._df_data_component_builder_cls = df_data_component_builder_cls
        self._file_data_component_builder_cls = file_data_component_builder_cls

        # Components
        self._passport = None
        self._data = None

        self._repo = self._workspace.dataset_repo
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

    @property
    def passport(self) -> DatasetPassport:
        """
        Provides access to the constructed passport for the dataset.

        Returns:
            DatasetPassport: The dataset's passport object.
        """
        return self._passport

    def with_passport(self) -> DatasetPassportBuilder:
        """
        Initializes the passport builder for configuring the dataset's passport.

        Returns:
            DatasetPassportBuilder: The builder for configuring the dataset passport.
        """
        self._passport = self._passport_builder

    @property
    def from_dataframe(self) -> DataComponentBuilder:
        """
        Initializes the data component builder for constructing the dataset
        from an in-memory dataframe.

        Returns:
            DataComponentBuilder: The builder for configuring the data component
            from a dataframe.
        """
        self._check_passport()
        return self._df_data_component_builder_cls(
            passport=self._passport, workspace=self._workspace
        ).dataframe

    @property
    def from_file(self) -> DataComponentBuilder:
        """
        Initializes the data component builder for constructing the dataset
        from a file.

        Returns:
            DataComponentBuilder: The builder for configuring the data component
            from a file.
        """
        self._check_passport()
        return self._file_data_component_builder_cls(
            passport=self._passport, workspace=self._workspace
        ).file

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
            ops=self._ops,
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
        msg += "Dataset operations not set\n" if self._ops is None else ""
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
