#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/component/ops.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 06:22:40 am                                               #
# Modified   : Saturday December 28th 2024 11:10:37 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Operations Module"""
from __future__ import annotations

import tempfile

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.component.data import DataComponent, DataFrameFileConfig
from discover.infra.persist.file.fao import FAO
from discover.infra.service.spark.pool import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
class DatasetOps:
    """
    A facade for dataset operations, including conversion, merging, splitting, sampling, and selection.

    Provides access to specialized operators through properties, ensuring a single, predictable interface.
    """

    def __init__(
        self,
        converter: ConvertOperator,
        merger: MergeOperator,
        splitter: SplitOperator,
        sampler: SampleOperator,
        selector: SelectOperator,
    ) -> None:
        self._converter = converter
        self._merger = merger
        self._splitter = splitter
        self._sampler = sampler
        self._selector = selector

    @property
    def converter(self) -> ConvertOperator:
        """Access the conversion operator."""
        return self._converter

    @property
    def merger(self) -> MergeOperator:
        """Access the merge operator."""
        return self._merger

    @property
    def splitter(self) -> SplitOperator:
        """Access the split operator."""
        return self._splitter

    @property
    def sampler(self) -> SampleOperator:
        """Access the sampling operator."""
        return self._sampler

    @property
    def selector(self) -> SelectOperator:
        """Access the selection operator."""
        return self._selector


# ------------------------------------------------------------------------------------------------ #
class ConvertOperator:

    def __init__(
        self, config: dict, fao: FAO, spark_session_pool: SparkSessionPool
    ) -> None:
        self._config = config
        self._fao = fao
        self._spark_session_pool = spark_session_pool

    # -------------------------------------------------------------------------------------------- #
    def to_pandas(self, source: DataComponent) -> pd.DataFrame:
        """
        Converts a DataComponent to the specified pandas DataFrame.

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            pd.DataFrame: The converted dataframe.
        """
        if source.dftype == DFType.PANDAS:
            return source.data  # No conversion needed

        if source.size <= self._config["to_pandas_threshold"]:
            return self._direct_to_pandas(source)
        else:
            return self._io_to_pandas(source=source)

    # -------------------------------------------------------------------------------------------- #
    def to_spark(self, source: DataComponent) -> DataFrame:
        if source.dftype == DFType.SPARK:
            return source.data  # No conversion needed

        if source.size <= self._config["to_spark_threshold"]:
            return self._direct_convert_to_spark(source)
        else:
            return self._io_to_spark(source=source)

    # -------------------------------------------------------------------------------------------- #
    def to_sparknlp(self, source: DataComponent) -> DataFrame:
        if source.dftype == DFType.SPARKNLP:
            return source.data  # No conversion needed

        if source.size <= self._config["to_spark_threshold"]:
            return self._direct_convert_to_sparknlp(source)
        else:
            return self._io_to_sparknlp(source=source)

    # -------------------------------------------------------------------------------------------- #
    #                             DIRECT CONVERSION METHODS                                        #
    # -------------------------------------------------------------------------------------------- #
    def _direct_to_pandas(self, source: DataComponent) -> pd.DataFrame:
        """
        Converts the given DataComponent directly to a Pandas DataFrame.

        This method uses PySpark's toPandas conversion facility.

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            pd.DataFrame: The converted Pandas DataFrame.
        """
        return source.data.toPandas()

    # -------------------------------------------------------------------------------------------- #
    def _direct_to_spark(self, source: DataComponent) -> DataFrame:
        """
        Converts the given DataComponent to a Spark DataFrame.

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            DataFrame: The converted Spark DataFrame.
        """
        spark = self._get_spark_session(dftype=DFType.SPARK)
        return spark.createDataFrame(source.data)

    # -------------------------------------------------------------------------------------------- #
    def _direct_to_sparknlp(self, source: DataComponent) -> DataFrame:
        """
        Converts the given DataComponent to a SparkNLP DataFrame.

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            DataFrame: The converted SparkNLP DataFrame.
        """
        spark = self._get_spark_session(dftype=DFType.SPARKNLP)
        return spark.createDataFrame(source.data)

    # -------------------------------------------------------------------------------------------- #
    #                       INDIRECT (IO-Based) CONVERSION METHODS                                 #
    # -------------------------------------------------------------------------------------------- #
    def _io_to_pandas(self, source: DataComponent) -> pd.DataFrame:
        """
        Converts the given DataComponent to a Pandas DataFrame.

        This method uses io to convert large DataFrames

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            pd.DataFrame: The converted Pandas DataFrame.
        """
        return self._io_convert(source=source, target_dftype=DFType.PANDAS)

    # -------------------------------------------------------------------------------------------- #
    def _io_to_spark(self, source: DataComponent) -> pd.DataFrame:
        """
        Converts the given DataComponent to a Spark DataFrame.

        This method uses io to convert large DataFrames

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            DataFrame: The converted Spark DataFrame.
        """
        return self._io_convert(source=source, target_dftype=DFType.SPARK)

    # -------------------------------------------------------------------------------------------- #
    def _io_to_sparknlp(self, source: DataComponent) -> pd.DataFrame:
        """
        Converts the given DataComponent to a SparkNLP DataFrame.

        This method uses io to convert large DataFrames

        Args:
            source (DataComponent): The source DataComponent.

        Returns:
            DataFrame: The converted SparkNLP DataFrame.
        """
        return self._io_convert(source=source, target_dftype=DFType.SPARKNLP)

    # -------------------------------------------------------------------------------------------- #
    #                          IO-BASED CONVERTER                                                  #
    # -------------------------------------------------------------------------------------------- #
    def _io_convert(self, source: DataComponent, target_dftype: DFType) -> DataFrame:
        """
        Performs an I/O-based conversion using Parquet as an intermediate format.

        Args:
            source (DataComponent): The source DataComponent.
            target_dftype (DFType): The target dataframe structure.

        Returns:
            Union[pd.DataFrame, DataFrame]: The converted dataframe.
        """

        with tempfile.TemporaryDirectory() as tempdir:
            temp_file = f"{tempdir}/temp_conversion.parquet"

            # Create an interim data_envelope pointing to tempfile.
            tempframe = DataComponent(
                data=source.data,
                filepath=temp_file,
                dftype=source.dftype,
                file_format=FileFormat.PARQUET,
            )
            # Write data to file
            self._fao.create(data_envelope=tempframe, overwrite=True)

            # Create the target DataFrameFileConfig object
            target_data_envelope_config = DataFrameFileConfig(
                filepath=temp_file,
                dftype=target_dftype,
                file_format=FileFormat.PARQUET,
            )
            # Convert to Spark if requested
            if target_data_envelope_config.dftype in (
                DFType.SPARK,
                DFType.SPARKNLP,
            ):
                spark = self._get_spark_session(dftype=target_dftype)
                # Read the dataframe back as a spark DataFrame
                return self._fao.read(
                    data_envelope_config=target_data_envelope_config, spark=spark
                )

            # Otherwise read back as pandas DataFrame
            else:
                return self._fao.read(data_envelope_config=target_data_envelope_config)

    # -------------------------------------------------------------------------------------------- #
    def _get_spark_session(self, dftype: DFType) -> SparkSession:
        if dftype == DFType.SPARK:
            return self._spark_session_pool.spark
        else:
            return self._spark_session_pool.sparknlp


# ------------------------------------------------------------------------------------------------ #
class MergeOperator:
    """Merges Dataframes"""


# ------------------------------------------------------------------------------------------------ #
class SplitOperator:
    """Splits Dataframes"""


# ------------------------------------------------------------------------------------------------ #
class SampleOperator:
    """Samples Dataframes"""


# ------------------------------------------------------------------------------------------------ #
class SelectOperator:
    """Selects rows from Dataframes"""
