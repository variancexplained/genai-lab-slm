#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/builder/data.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:20:36 pm                                               #
# Modified   : Saturday December 28th 2024 07:18:45 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Component Builder Module"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.base import DatasetComponentBuilder
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport
from discover.infra.persist.file.fao import FAO
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.utils.file.copy import Copy
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
#                               DATA COMPONENT BUILDER                                             #
# ------------------------------------------------------------------------------------------------ #
class DataComponentBuilder(DatasetComponentBuilder):

    def __init__(self, passport: DatasetPassport, workspace: Workspace):
        self._passport = passport
        self._workspace = workspace
        self._data = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._created = None

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _validate(self) -> None:
        """
        Validates the builder's current state to ensure consistency and integrity.

        """
        # Filepath Validation
        if not isinstance(self._filepath, str):
            msg = f"File path `filepath` is missing or not a valid string. Filepath type: {type(self._filepath)}."
            self._logger.error(msg)
            self.reset()
            raise TypeError(msg)

        # File format validation or infer if not provided.
        if not self._file_format:
            if "csv" in self._filepath:
                self._file_format = FileFormat.CSV
                self._logger.warning(
                    f"File format `file_format` was inferred from the filepath as {self._file_format.value}"
                )
            else:
                self._file_format = FileFormat.PARQUET
                self._logger.warning(
                    f"File format `file_format` was inferred from the filepath as {self._file_format.value}"
                )
        else:
            if self._file_format != FileFormat.CSV and "csv" in self._filepath:
                msg = f"Incompatiable file_format: {self._file_format.value} and filepath {os.path.basename(self._filepath)}. Expected FileFormat.CSV"
                self._logger.error(msg)
                self.reset()
                raise ValueError(msg)
            elif self._file_format == FileFormat.CSV and "parquet" in self._filepath:
                msg = f"Incompatiable file_format: {self._file_format.value} and filepath {os.path.basename(self._filepath)}. Expected FileFormat.PARQUET"
                self._logger.error(msg)
                self.reset()
                raise ValueError(msg)

        # DataFrame Type Validation
        if self._data is not None:
            if not isinstance(
                self._data, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
            ):
                msg = f"TypeError: `data` is type {type(self._data)}. Expected a pandas or spark DataFrame"
                self._logger.error(msg)
                self.reset()
                raise TypeError(msg)

        # Validate or infer Dataframe Type `dftype`
        if self._dftype is None:
            if isinstance(self._data, (pd.DataFrame, pd.core.frame.DataFrame)):
                self._dftype = DFType.PANDAS
                self._logger.warning(
                    f"DataFrame type `dftype` has been inferred from the data as {self._dftype.value}"
                )
            elif isinstance(self._data, DataFrame):
                self._dftype = DFType.SPARK
                self._logger.warning(
                    f"DataFrame type `dftype` has been inferred from the data as {self._dftype.value}"
                )
            else:
                msg = "ValueErrror: DataFrame type `dftype` is None and could not be inferred from the data."
                self._logger.error(msg)
                self.reset()
                raise ValueError(msg)
        else:
            if (
                isinstance(self._data, (pd.DataFrame, pd.core.frame.DataFrame))
                and self._dftype != DFType.PANDAS
            ):
                msg = f"DataIntegrityError: Data and DataFrame type are not compatible.\nDataFrame is type {type(self._data)} and `dftype` is {self._dftype.value}."
                self._logger.error(msg)
                self.reset()
                raise TypeError(msg)

            elif isinstance(self._data, DataFrame) and self._dftype == DFType.PANDAS:
                msg = f"DataIntegrityError: Data and DataFrame type are not compatible.\nDataFrame is type {type(self._data)} and `dftype` is {self._dftype.value}."
                self._logger.error(msg)
                self.reset()
                raise TypeError(msg)


# ------------------------------------------------------------------------------------------------ #
#                          DATAFRAME SOURCE DATA COMPONENT BUILDER                                 #
# ------------------------------------------------------------------------------------------------ #
class DFSourceDataComponentBuilder(DataComponentBuilder):
    """
    A builder class for constructing a `DataComponent` object.

    This builder provides a fluent interface to configure and construct a `DataComponent`
    that encapsulates the data, its type, file format, and associated metadata.

    Args:
        passport (DatasetPassport): The dataset's passport, containing metadata about
            the dataset (e.g., asset ID, phase, stage, name).
        workspace (Workspace): The workspace object for managing file paths and asset tracking.

    Attributes:
        data_component (DataComponent): The constructed `DataComponent` object.
            Accessing this property resets the builder's internal state.

    Methods:
        reset() -> None:
            Resets the builder's internal state.

        data(data: Union[pd.DataFrame, DataFrame]) -> DFSourceDataComponentBuilder:
            Sets the data for the `DataComponent`.

        pandas() -> DFSourceDataComponentBuilder:
            Specifies that the data type is a Pandas DataFrame.

        spark() -> DFSourceDataComponentBuilder:
            Specifies that the data type is a Spark DataFrame.

        sparknlp() -> DFSourceDataComponentBuilder:
            Specifies that the data type is a SparkNLP DataFrame.

        to_csv() -> DFSourceDataComponentBuilder:
            Sets the file format to CSV and determines the file path.

        to_parquet() -> DFSourceDataComponentBuilder:
            Sets the file format to Parquet and determines the file path.

        build() -> DFSourceDataComponentBuilder:
            Validates and constructs the `DataComponent` object.

    Internal Methods:
        _get_filepath() -> str:
            Generates the file path for the data component using the workspace and passport.

        _validate() -> None:
            Validates the builder's current state to ensure consistency and integrity.

    Raises:
        ValueError: If validation fails due to missing or invalid attributes.
    """

    def __init__(self, passport: DatasetPassport, workspace: Workspace):
        super().__init__(passport=passport, workspace=workspace)

    @property
    def data_component(self) -> DataComponent:
        """
        Gets the constructed `DataComponent` object and resets the builder's state.

        Returns:
            DataComponent: The constructed `DataComponent` object.
        """
        data_component = self._data_component
        self.reset()
        return data_component

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._data = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None

    def data(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> DFSourceDataComponentBuilder:
        """
        Sets the data for the `DataComponent`.

        Args:
            data (Union[pd.DataFrame, DataFrame]): The data to be included in the `DataComponent`.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._data = data
        return self

    def pandas(self) -> DFSourceDataComponentBuilder:
        """
        Specifies that the data type is a Pandas DataFrame.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    def spark(self) -> DFSourceDataComponentBuilder:
        """
        Specifies that the data type is a Spark DataFrame.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    def sparknlp(self) -> DFSourceDataComponentBuilder:
        """
        Specifies that the data type is a SparkNLP DataFrame.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARKNLP
        return self

    def to_csv(self) -> DFSourceDataComponentBuilder:
        """
        Sets the file format to CSV and determines the file path.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        self._filepath = self._get_filepath()
        return self

    def to_parquet(self) -> DFSourceDataComponentBuilder:
        """
        Sets the file format to Parquet and determines the file path.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        self._filepath = self._get_filepath()
        return self

    def build(self) -> DFSourceDataComponentBuilder:
        """
        Validates and constructs the `DataComponent` object.

        Returns:
            DFSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._created = datetime.now()
        self._validate()
        self._data_component = DataComponent(
            dftype=self._dftype,
            filepath=self._filepath,
            file_format=self._file_format,
            created=self._created,
            _data=self._data,
        )
        return self

    def _get_filepath(self) -> str:
        """
        Generates the file path for the data component using the workspace and passport.

        Returns:
            str: The file path for the data component.
        """
        return self._workspace.get_filepath(
            asset_id=self._passport.asset_id, file_format=self._file_format
        )

    def _validate(self) -> None:
        """
        Validates the builder's current state to ensure consistency and integrity.

        """
        super()._validate()
        if not isinstance(
            self._data, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            msg = "Missing or invalid dataframe."
            self._logger.error(msg)
            self.reset()
            raise TypeError(msg)


# ------------------------------------------------------------------------------------------------ #
#                            FILE SOURCE DATA COMPONENT BUILDER                                    #
# ------------------------------------------------------------------------------------------------ #
class FileSourceDataComponentBuilder(DataComponentBuilder):
    """
    A builder class for constructing a `DataComponent` object from file sources.

    This builder provides a fluent interface for configuring and constructing a
    `DataComponent` that encapsulates file-based data, its type, file format,
    and associated metadata. It supports both lazy and eager loading of data,
    automatically manages workspace file paths, and validates configurations.

    Args:
        passport (DatasetPassport): Metadata for the dataset, including asset ID, phase,
            stage, and name.
        workspace (Workspace): Manages file paths, asset tracking, and related operations.
        fao (FAO): The file access object for reading and writing dataset files.

    Attributes:
        data_component (DataComponent): The constructed `DataComponent` object.
            Accessing this property resets the builder's internal state.

    Methods:
        reset() -> None:
            Resets the builder's internal state.

        lazy_loading() -> FileSourceDataComponentBuilder:
            Enables lazy loading for the `DataComponent`.

        filepath(filepath: str) -> FileSourceDataComponentBuilder:
            Sets the file path for the data source.

        file_format(file_format: FileFormat) -> FileSourceDataComponentBuilder:
            Sets the file format (e.g., CSV, Parquet) for the data source.

        as_pandas() -> FileSourceDataComponentBuilder:
            Specifies that the data type is a Pandas DataFrame.

        as_spark() -> FileSourceDataComponentBuilder:
            Specifies that the data type is a Spark DataFrame.

        as_sparknlp() -> FileSourceDataComponentBuilder:
            Specifies that the data type is a SparkNLP DataFrame.

        csv() -> FileSourceDataComponentBuilder:
            Sets the file format to CSV.

        parquet() -> FileSourceDataComponentBuilder:
            Sets the file format to Parquet.

        build() -> FileSourceDataComponentBuilder:
            Constructs the `DataComponent` object, validating the configuration,
            optionally loading the data eagerly, and copying the file to the workspace.

    Internal Methods:
        _validate() -> None:
            Validates the builder's current state to ensure consistency and integrity.

        _copy_file_to_workspace() -> str:
            Copies the source file to the workspace and returns the updated file path.

        _load_data() -> Union[pd.DataFrame, DataFrame]:
            Loads the data based on the configured dataframe type and file format.

        _load_pandas_data() -> pd.DataFrame:
            Loads the data as a Pandas DataFrame.

        _load_spark_data(spark_session_pool: SparkSessionPool) -> DataFrame:
            Loads the data as a Spark DataFrame using the Spark session pool.

    Raises:
        ValueError: If validation fails due to missing or inconsistent attributes.
    """

    def __init__(
        self,
        passport: DatasetPassport,
        workspace: Workspace,
        fao: FAO,
        spark_session_pool: SparkSessionPool,
    ):
        super().__init__(passport=passport, workspace=workspace)
        self._fao = fao
        self._spark_session_pool = spark_session_pool
        self._lazy_loading = False

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def data_component(self) -> DataComponent:
        """
        Gets the constructed `DataComponent` object and resets the builder's state.

        Returns:
            DataComponent: The constructed `DataComponent` object.
        """
        data_component = self._data_component
        self.reset()
        return data_component

    def reset(self) -> None:
        """
        Resets the builder's internal state.

        Clears all attributes, preparing the builder for a new configuration.
        """
        self._data = None
        self._data_component = None
        self._dftype = None
        self._file_format = None
        self._filepath = None
        self._lazy_loading = False

    def lazy_loading(self) -> FileSourceDataComponentBuilder:
        """
        Enables lazy loading for the `DataComponent`.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._lazy_loading = True
        return self

    def filepath(self, filepath: str) -> FileSourceDataComponentBuilder:
        """
        Sets the file path for the data source.

        Args:
            filepath (str): The path to the data file.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._filepath = filepath
        return self

    def file_format(self, file_format: FileFormat) -> FileSourceDataComponentBuilder:
        """
        Sets the file format for the data source.

        Args:
            file_format (FileFormat): The format of the file (e.g., CSV, Parquet).

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = file_format
        return self

    def as_pandas(self) -> FileSourceDataComponentBuilder:
        """
        Specifies that the data type is a Pandas DataFrame.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.PANDAS
        return self

    def as_spark(self) -> FileSourceDataComponentBuilder:
        """
        Specifies that the data type is a Spark DataFrame.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARK
        return self

    def as_sparknlp(self) -> FileSourceDataComponentBuilder:
        """
        Specifies that the data type is a SparkNLP DataFrame.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._dftype = DFType.SPARKNLP
        return self

    def csv(self) -> FileSourceDataComponentBuilder:
        """
        Sets the file format to CSV.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.CSV
        return self

    def parquet(self) -> FileSourceDataComponentBuilder:
        """
        Sets the file format to Parquet.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._file_format = FileFormat.PARQUET
        return self

    def build(self) -> FileSourceDataComponentBuilder:
        """
        Constructs the `DataComponent` object.

        Validates the current configuration, optionally loads the data eagerly,
        and creates the `DataComponent` with the specified attributes.

        Returns:
            FileSourceDataComponentBuilder: The current builder instance for chaining.
        """
        self._created = datetime.now()
        self._validate()

        # The filepath provided is not in the workspace. The following determines a filepath for
        # the component, copies the source file to the workspace and returns its workspace
        # filepath.
        self._filepath = self._copy_file_to_workspace()

        # Load data if eager loading
        if not self._lazy_loading:
            self._data = self._load_data()

        self._data_component = DataComponent(
            dftype=self._dftype,
            filepath=self._filepath,
            file_format=self._file_format,
            created=self._created,
            _data=self._data,
        )
        return self

    def _copy_file_to_workspace(self) -> str:
        """
        Copies the source file to the workspace and returns the updated file path.

        Returns:
            str: The file path of the copied file in the workspace.
        """
        filepath = self._workspace.get_filepath(
            self._passport.asset_id, file_format=self._file_format
        )
        copy = Copy()
        copy(source=self._filepath, target=filepath)
        return filepath

    def _load_data(self) -> Union[pd.DataFrame, DataFrame]:
        """
        Loads the data based on the configured dataframe type and file format.

        Returns:
            Union[pd.DataFrame, DataFrame]: The loaded data as a Pandas or Spark DataFrame.
        """
        if self._dftype == DFType.PANDAS:
            return self._load_pandas_data()
        else:
            return self._load_spark_data()

    def _load_pandas_data(self) -> pd.DataFrame:
        """
        Loads the data as a Pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded data as a Pandas DataFrame.
        """
        return self._fao.read(
            filepath=self._filepath, file_format=self._file_format, dftype=self._dftype
        )

    def _load_spark_data(
        self,
    ) -> DataFrame:
        """
        Loads the data as a Spark DataFrame using the Spark session pool.

        Args:
            spark_session_pool (SparkSessionPool): The pool of Spark sessions for managing
                Spark-based operations.

        Returns:
            DataFrame: The loaded data as a Spark DataFrame.
        """
        spark = (
            self._spark_session_pool.spark
            if self._dftype == DFType.SPARK
            else self._spark_session_pool.sparknlp
        )
        return self._fao.read(
            filepath=self._filepath,
            file_format=self._file_format,
            dftype=self._dftype,
            spark=spark,
        )
