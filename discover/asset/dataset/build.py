#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/build.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 01:41:03 am                                               #
# Modified   : Friday December 27th 2024 10:20:51 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Dataset Builder Moddule"""
from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset import Dataset
from discover.asset.dataset.base import DatasetBuilder as BaseDatasetBuilder
from discover.asset.dataset.ops import DatasetOps
from discover.core.asset import AssetType
from discover.core.data_structure import DataEnvelope, DataFrameStructureEnum
from discover.core.file import FileFormat
from discover.core.flow import PhaseEnum, StageEnum
from discover.core.identity import DatasetPassport
from discover.infra.exception.dataset import DatasetBuilderError
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilder(BaseDatasetBuilder):
    """
    Abstract base class for building datasets with configurable phases, stages,
    and formats. The builder pattern supports ingesting data from multiple sources,
    configuring internal representations, and exporting to various formats.
    """

    __asset_type = AssetType.DATASET

    def __init__(self, workspace: Workspace, ops: DatasetOps) -> None:
        self._workspace = workspace
        self._ops = ops

        self._phase = None
        self._stage = None
        self._name = None
        self._repo = self._workspace.dataset_repo
        self._data = None
        self._source_dataframe_structure = DataFrameStructureEnum.SPARK
        self._source_file_format = FileFormat.PARQUET
        self._source_filepath = None
        self._target_dataframe_structure = DataFrameStructureEnum.SPARK
        self._target_file_format = FileFormat.PARQUET

        self._dataset = None

    @property
    def dataset(self) -> Dataset:
        dataset = self._dataset
        self.reset()
        return dataset

    def reset(self) -> None:
        self._phase = None
        self._stage = None
        self._name = None
        self._repo = self._workspace.dataset_repo
        self._data = None
        self._source_dataframe_structure = None
        self._source_file_format = None
        self._source_filepath = None
        self._target_dataframe_structure = None
        self._target_file_format = None

        self._dataset = None

    def for_phase(self, phase: PhaseEnum) -> DatasetBuilder:
        """
        Configures the dataset for a specific phase (e.g., training, testing).

        Args:
            phase (PhaseEnum): The phase of the dataset lifecycle.

        Returns:
            DatasetBuilder: The builder instance with the phase configuration applied.
        """
        self._phase = phase
        return self

    def for_stage(self, stage: StageEnum) -> DatasetBuilder:
        """
        Configures the dataset for a specific stage (e.g., preprocessing, analysis).

        Args:
            stage (StageEnum): The stage of the dataset lifecycle.

        Returns:
            DatasetBuilder: The builder instance with the stage configuration applied.
        """
        self._stage = stage
        return self

    def with_name(self, name: str) -> DatasetBuilder:
        """
        Assigns a name to the dataset for identification purposes.

        Args:
            name (str): The name of the dataset.

        Returns:
            DatasetBuilder: The builder instance with the name configuration applied.
        """
        self._name = name
        return self

    def from_pandas_dataframe(self, data: pd.DataFrame) -> DatasetBuilder:
        """
        Ingests data from an in-memory Pandas DataFrame.

        Args:
            data (pd.DataFrame): The Pandas DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        self._source_dataframe_structure = DataFrameStructureEnum.PANDAS
        self._data = data
        return self

    def from_spark_dataframe(self, data: DataFrame) -> DatasetBuilder:
        """
        Ingests data from an in-memory Spark DataFrame.

        Args:
            data (DataFrame): The Spark DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        self._source_dataframe_structure = DataFrameStructureEnum.SPARK
        self._data = data
        return self

    def from_sparknlp_dataframe(self, data: DataFrame) -> DatasetBuilder:
        """
        Ingests data from a Spark NLP DataFrame.

        Args:
            data (DataFrame): The Spark NLP DataFrame to use as the dataset source.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        self._source_dataframe_structure = DataFrameStructureEnum.SPARKNLP
        self._data = data
        return self

    def from_parquet_file(self, filepath: str) -> DatasetBuilder:
        """
        Ingests data from a Parquet file.

        Args:
            filepath (str): The path to the Parquet file.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        self._source_file_format = FileFormat.PARQUET
        self._source_filepath = filepath
        return self

    def from_csv_file(self, filepath: str) -> DatasetBuilder:
        """
        Ingests data from a CSV file.

        Args:
            filepath (str): The path to the CSV file.

        Returns:
            DatasetBuilder: The builder instance with the data ingested.
        """
        self._source_file_format = FileFormat.CSV
        self._source_filepath = filepath
        return self

    def as_pandas_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Pandas DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Pandas.
        """
        self._target_dataframe_structure = DataFrameStructureEnum.PANDAS
        return self

    def as_spark_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Spark DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Spark.
        """
        self._target_dataframe_structure = DataFrameStructureEnum.SPARK
        return self

    def as_sparknlp_dataframe(self) -> DatasetBuilder:
        """
        Configures the dataset's internal representation as a Spark NLP DataFrame.

        Returns:
            DatasetBuilder: The builder instance with the internal form set to Spark NLP.
        """
        self._target_dataframe_structure = DataFrameStructureEnum.SPARKNLP
        return self

    def to_csv_format(self) -> DatasetBuilder:
        """
        Exports the dataset in CSV format.

        Returns:
            DatasetBuilder: The builder instance with the export action completed.
        """
        self._target_file_format = FileFormat.CSV
        return self

    def to_parquet_format(self) -> DatasetBuilder:
        """
        Exports the dataset in Parquet format.

        Returns:
            DatasetBuilder: The builder instance with the export action completed.
        """
        self._target_file_format = FileFormat.PARQUET
        return self

    def build(self) -> Dataset:
        """
        Builds and returns the final Dataset object based on the provided configurations.

        Returns:
            Dataset: The fully constructed dataset.
        """
        self._validate()

        # Load data
        if self._data is None:
            self._load_data_from_file()

        # Construct the passport
        passport = self._build_passport()

        # Construct the data_envelope object
        data_envelope = self._build_data_envelope(passport=passport)

        # Construct the Dataset
        dataset = self._build_dataset(passport=passport, data_envelope=data_envelope)

        # Register Dataset
        self._register_dataset(dataset=dataset)

        # Present constructed dataset as property
        self._dataset = dataset

    def _load_data_from_file(self) -> None:
        """Constructs a Dataset from file"""

        self._data = self._workspace.dataset_repo.get_data(
            filepath=self._source_filepath,
            file_format=self._source_file_format,
            dataframe_structure=self._source_dataframe_structure,
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
            dataframe_structure=self._target_dataframe_structure,
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

    def _validate(self) -> None:
        msg = ""
        msg += "The phase argument is required.\n" if self._phase is None else ""
        msg += "The stage argument is required.\n" if self._stage is None else ""
        msg += "The name argument is required.\n" if self._name is None else ""
        msg += (
            "The target_filepath argument is required.\n"
            if self._target_filepath is None
            else ""
        )
        msg += (
            "The target_file_format argument is required.\n"
            if self._target_file_format is None
            else ""
        )
        msg += (
            "The target_dataframe_structure argument is required.\n"
            if self._target_dataframe_structure is None
            else ""
        )
        if self._data is None:

            msg += (
                "The source_filepath argument is required.\n"
                if self._source_filepath is None
                else ""
            )
            msg += (
                "The source_file_format argument is required.\n"
                if self._source_file_format is None
                else ""
            )
            msg += (
                "The source_dataframe_structure argument is required.\n"
                if self._source_dataframe_structure is None
                else ""
            )
        if msg:
            raise DatasetBuilderError(msg)
