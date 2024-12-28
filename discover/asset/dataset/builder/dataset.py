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
# Modified   : Saturday December 28th 2024 02:21:36 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Core Module"""
from __future__ import annotations

from typing import Type

from discover.asset.base import AssetBuilder
from discover.asset.dataset.builder.data import DataFrameFileConfigBuilder
from discover.asset.dataset.builder.identity import DatasetPassportBuilder
from discover.asset.dataset.component.data import DataEnvelope, DataFrameFileConfig
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
        data_builder_cls: Type[DataFrameFileConfigBuilder] = DataFrameFileConfigBuilder,
    ) -> None:
        # Internal resources
        self._workspace = workspace
        # Dataset Components
        self._passport = None
        self._source = None
        self._target = None
        self._ops = ops
        self._repo = self._workspace.dataset_repo

        self._dataset = None

        # Component Builders
        self._passport_builder_cls = passport_builder_cls
        self._df_io_spec_builder = df_io_spec_builder_cls

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
    def source(self) -> DatasetPassportBuilder:
        return self.data_builder

    @property
    def df_io_spec(self) -> DataFrameFileConfigBuilder:
        return self._df_spec_builder

    @property
    def passport_ref(self) -> DatasetPassport:
        """Provides sub builders with access to the passport ."""
        return self._passport

    @property
    def workspace(self) -> Workspace:
        """Provides access component builders access to the workspace."""
        return self._workspace

    def build(self) -> DatasetBuilder:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """

        # Load data
        if self._data is None:
            self._load_data_from_file()

        # Construct the passport
        self._passport = self._build_passport()

        # Construct source and target data configs
        self._build_source_data_config()

        # Construct the data_envelope object
        self._data_envelope = self._build_data_envelope(passport=self._passport)

        # Construct the data envelop config
        self._data_envelope_config = DataFrameFileConfig(
            filepath=self._data_envelope.filepath,
            dftype=self._data_envelope.dftype,
            file_format=self._data_envelope.file_format,
        )

        # Construct the Dataset
        dataset = self._build_dataset(
            passport=self._passport, data_envelope=self._data_envelope
        )

        # Validate the dataset
        self._validate(dataset)

        # Register Dataset
        self._workspace.dataset_repo.add(asset=dataset)

        # Present constructed dataset as property
        self._dataset = dataset

    def _load_data_from_file(self) -> None:
        """Constructs a Dataset from file"""

        self._data = self._workspace.dataset_repo.get_data(
            data_envelope_config=self._data_envelope_config,
        )

    def _build_passport(self) -> DatasetPassport:
        asset_id = self._workspace.get_asset_id(
            asset_type=self.__asset_type,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
        )
        return DatasetPassport(
            asset_id=asset_id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            asset_type=self.__asset_type,
        )

    def _build_data_envelope(self, passport: DatasetPassport) -> DataEnvelope:
        filepath = self._workspace.get_filepath(
            asset_id=passport.asset_id, file_format=self._target_file_format
        )
        return DataEnvelope(
            data=self._data,
            filepath=filepath,
            dftype=self._target_dftype,
            file_format=self._target_file_format,
        )

    def _build_dataset(
        self, passport: DatasetPassport, data_envelope: DataEnvelope
    ) -> None:
        """Constructs the Dataset object"""
        return Dataset(
            passport=passport,
            workspace=self._workspace,
            data_envelope=data_envelope,
            ops=self._ops,
        )

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
