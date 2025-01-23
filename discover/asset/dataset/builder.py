#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:20:36 pm                                               #
# Modified   : Thursday January 23rd 2025 06:07:59 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pyspark.sql import DataFrame

from discover.asset.base.asset import AssetType
from discover.asset.base.builder import AssetBuilder
from discover.asset.dataset.dataframer import (
    DataFramer,
    PandasDataFramer,
    PySparkDataFramer,
)
from discover.asset.dataset.dataset import Dataset, DatasetConfig
from discover.asset.dataset.identity import DatasetPassport
from discover.container import DiscoverContainer
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.file.fao import FAO
from discover.infra.utils.file.fileset import FileFormat


# ================================================================================================ #
#                                   DATASET BUILDER                                                 #
# ================================================================================================ #
class DatasetBuilder(AssetBuilder):

    __ASSET_TYPE = AssetType.DATASET

    @inject
    def __init__(
        self,
        repo: DatasetRepo = Provide[DiscoverContainer.io.repo],
        fao: FAO = Provide[DiscoverContainer.io.fao],
    ) -> None:
        super().__init__()
        self._repo = repo
        self._fao = fao

        self.reset()

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """

        self._asset_type = self.__ASSET_TYPE
        self._dataframe = None
        self._phase = None
        self._stage = None
        self._name = "review"
        self._description = None
        self._creator = None
        self._dftype = None
        self._source = None
        self._source_filepath = None
        self._file_format = FileFormat.PARQUET
        self._dataframer = None

        self._passport = None
        self._dataset = None

    # -------------------------------------------------------------------------------------------- #
    #                                  PASSPORT INFO                                               #
    # -------------------------------------------------------------------------------------------- #
    def name(self, name: str) -> DatasetBuilder:
        """Sets the name of the Dataset

        Args:
            name (str): The name of the dataset.

        Returns:
            DatasetBuilder: The current builder instance.
        """
        self._name = name
        return self

    # -------------------------------------------------------------------------------------------- #
    def description(self, description: str) -> DatasetBuilder:
        """Sets the description of the Dataset

        Args:
            description (str): The description of the dataset.

        Returns:
            DatasetBuilder: The current builder instance.
        """
        self._description = description
        return self

    # -------------------------------------------------------------------------------------------- #
    def phase(self, phase: PhaseDef = PhaseDef.DATAPREP) -> DatasetBuilder:
        """Sets the phase for which the Dataset is being created.

        Args:
            phase (PhaseDef): The project phase for which the dataset is being constructed.
                Defaults to DataPrep phase.

        Returns:
            DatasetBuilder: The current builder instance.
        """
        self._phase = phase
        return self

    # -------------------------------------------------------------------------------------------- #
    def stage(self, stage: StageDef) -> DatasetBuilder:
        """Sets the stage for which the Dataset is being created.

        Args:
            stage (StageDef): The stage for which the dataset is being constructed.

        Returns:
            DatasetBuilder: The current builder instance.
        """
        self._stage = stage
        return self

    # -------------------------------------------------------------------------------------------- #
    def creator(self, creator: str) -> DatasetBuilder:
        """Sets the creator of the dataset.

        Args:
            creator (str): The creator of the dataset. This is typically the name
                of the class that initiated the build request.

        Returns:
            DatasetBuilder: The current builder instance.
        """
        self._creator = creator
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                 DATA SOURCE                                                  #
    # -------------------------------------------------------------------------------------------- #
    def from_config(self, config: DatasetConfig) -> DatasetBuilder:
        """
        Sets the Dataset configuration on the builder.

        Args:
            config (DatasetConfig): The configuration for the dataset.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._phase = config.phase
        self._stage = config.stage
        self._name = config.name
        self._file_format = config.file_format
        self._dftype = config.dftype
        self._creator = config.creator
        self._dataframe = config.dataframe
        return self

    # -------------------------------------------------------------------------------------------- #
    def dataframe(self, dataframe: Union[pd.DataFrame, DataFrame]) -> DatasetBuilder:
        """
        Sets the source as a Pandas or PySpark DataFrame.

        Args:
            dataframe (Union[pd.DataFrame, DataFrame]): Pandas or PySpark DataFrame

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dataframe = dataframe
        # Set the dftype parameter to a default based on the DataFrame type.
        if isinstance(dataframe, DataFrame):
            self._dftype = (
                self._dftype or DFType.SPARK
            )  # Defaults to Spark (rather than SparkNLP) if not set.
        else:
            self._dftype = self._dftype or DFType.PANDAS

        return self

    # -------------------------------------------------------------------------------------------- #
    def source(self, source: DatasetPassport) -> DatasetBuilder:
        """Sets the dataset source with the source DatasetPassport

        Args:
            source (DatasetPassport): The passport for the source of the dataset.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._source = source
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                  DATAFRAME TYPE                                              #
    # -------------------------------------------------------------------------------------------- #
    def as_pandas(self) -> DatasetBuilder:
        """
        Sets the DataFrame type to Pandas.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    # -------------------------------------------------------------------------------------------- #
    def as_spark(self) -> DatasetBuilder:
        """
        Sets the DataFrame type to Spark.

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    # -------------------------------------------------------------------------------------------- #
    def as_sparknlp(self) -> DatasetBuilder:
        """
        Sets the DataFrame type to SparkNLP

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARKNLP
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                  FILE FORMAT                                                 #
    # -------------------------------------------------------------------------------------------- #
    def to_csv(self) -> DatasetBuilder:
        """
        Sets the file format to csv

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    # -------------------------------------------------------------------------------------------- #
    def to_parquet(self) -> DatasetBuilder:
        """
        Sets the file format to parquet

        Returns:
            DatasetBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                      BUILD                                                   #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> DatasetBuilder:

        self._validate()

        self._passport = self._build_passport()

        self._dataframer = self._build_dataframer()

        self._dataset = Dataset(passport=self._passport, dataframer=self._dataframer)

        return self

    # -------------------------------------------------------------------------------------------- #
    def _build_passport(self) -> DatasetPassport:
        """Constructs the passport for the dataset."""
        # Generate an asset id the dataset.
        asset_id = self._repo.gen_asset_id(
            phase=self._phase, stage=self._stage, name=self._name
        )

        passport = DatasetPassport(
            assert_type=self._asset_type,
            asset_id=asset_id,
            phase=self._phase,
            stage=self._stage,
            name=self._name,
            description=self._description,
            creator=self._creator,
            created=datetime.now(),
            source=self._source,
            file_format=self._file_format,
            dftype=self._dftype,
        )

        return passport

    # -------------------------------------------------------------------------------------------- #
    def _build_dataframer(self) -> DataFramer:
        """Constructs the DataFramer object"""
        # If no dataframe were provided,read it from file.
        if not isinstance(
            self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            self._dataframe = self._read_data()

        # Instantiate the DataFramer for the DataFrame type
        if self._dftype == DFType.PANDAS:
            return PandasDataFramer(df=self._dataframe)
        else:
            return PySparkDataFramer(df=self._dataframe)

    # -------------------------------------------------------------------------------------------- #
    def _read_data(self) -> Union[pd.DataFrame, DataFrame]:
        """Reads a PySpark or Pandas DataFrame from file."""

        if self._dftype in (DFType.SPARK, DFType.SPARKNLP):
            try:
                spark = self._workspace.spark.session_pool().get_spark_session(
                    dftype=self._dftype
                )
                return self._fao.read(
                    filepath=self._source_filepath,
                    dftype=self._dftype,
                    spark=spark,
                )
            except Exception as e:
                msg = f"Exception encountered while reading from {self._source_filepath}.\n{e}"
                self._logger.error(msg)
                raise Exception(msg) from e
        else:
            return self._fao.read(filepath=self._source_filepath, dftype=self._dftype)

    # -------------------------------------------------------------------------------------------- #
    def _validate(self) -> None:
        errors = []

        # Validate name
        if not isinstance(self._name, str):
            errors.append(
                f"The dataset `name` is a required string property. Received a {type(self._name)} type."
            )

        # Validate phase
        if not isinstance(self._phase, PhaseDef):
            errors.append(
                f"The dataset `phase` is required PhaseDef property. Received a {type(self._phase)} type."
            )

        # Validate stage
        if not isinstance(self._stage, StageDef):
            errors.append(
                f"The dataset `stage` is required StageDef property. Received a {type(self._stage)} type."
            )

        # Validate File Format
        if not isinstance(self._file_format, FileFormat):
            errors.append(
                f"File format is a required FileFormat property. Received a {type(self._file_format)} type."
            )

        # Validate DFType
        if not isinstance(self._dftype, DFType):
            errors.append(
                f"The `dftype` is a required DFType property. Received a {type(self._dftype)} type."
            )

        # Validate the DataFrame
        if not isinstance(
            self._dataframe, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            errors.append(
                "A source of the data must be set via the `from_dataframe` or the `from_file` method."
            )

        if errors:
            self._report_validation_errors(errors=errors)
