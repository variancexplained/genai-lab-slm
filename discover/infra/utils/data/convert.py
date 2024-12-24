#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/utils/data/convert.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 12:30:01 am                                                 #
# Modified   : Monday December 23rd 2024 10:04:55 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""DataFrame Conversion Module"""
import logging
import os
import shutil
from typing import Optional

import pandas as pd
import psutil  # For system memory info
from pyspark.sql import DataFrame

from discover.infra.service.spark.pool import SparkSessionPool


# ------------------------------------------------------------------------------------------------ #
class DataFrameConverter:
    """Pandas and Spark DataFrame conversion with automatic optimization."""

    def __init__(self, spark_session_pool: SparkSessionPool) -> None:
        self._spark_session_pool = spark_session_pool
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _estimate_row_size(self, sdf: DataFrame, sample_size: float = 0.01) -> int:
        """Estimate the average row size in bytes (including handling text columns)."""
        self._logger.debug(
            f"Estimating row size by sampling {sample_size*100}% of the DataFrame"
        )
        sample_df = (
            sdf.sample(withReplacement=False, fraction=sample_size)
            .limit(1000)
            .toPandas()
        )
        row_size = 0
        for col in sample_df.columns:
            if pd.api.types.is_numeric_dtype(sample_df[col]):
                row_size += 8
            elif pd.api.types.is_string_dtype(sample_df[col]):
                avg_string_length = sample_df[col].str.len().mean()
                row_size += avg_string_length * 1.2
        return row_size

    def _determine_conversion_method(
        self,
        sdf: DataFrame,
        memory_fraction: float = 0.8,
        parquet_threshold: int = 500 * 1024 * 1024,
    ) -> str:
        """
        Determines whether to use direct conversion or Parquet-based conversion based on dataset size.

        Args:
            sdf (DataFrame): The Spark DataFrame.
            memory_fraction (float): Fraction of available memory to use for estimation.
            parquet_threshold (int): Threshold in bytes above which Parquet-based conversion is preferred.

        Returns:
            str: 'toPandas' or 'parquet' indicating the best conversion method.
        """
        total_rows = sdf.count()
        row_size_estimate = self._estimate_row_size(sdf)
        total_size_estimate = total_rows * row_size_estimate
        available_memory = psutil.virtual_memory().available * memory_fraction

        self._logger.debug(
            f"Dataset size estimate: {total_size_estimate / (1024 * 1024)} MB"
        )
        self._logger.debug(f"Available memory: {available_memory / (1024 * 1024)} MB")

        if (
            total_size_estimate < available_memory
            and total_size_estimate < parquet_threshold
        ):
            self._logger.debug("Using direct .toPandas() conversion.")
            return "toPandas"
        else:
            self._logger.debug("Using Parquet-based conversion.")
            return "parquet"

    def to_pandas(
        self,
        sdf: DataFrame,
        use_arrow: bool = False,
        memory_fraction: float = 0.8,
        parquet_threshold: int = 500 * 1024 * 1024,
    ) -> pd.DataFrame:
        """
        Converts a Spark DataFrame to a Pandas DataFrame, automatically choosing the best approach.

        Args:
            sdf (DataFrame): The Spark DataFrame to convert.
            use_arrow (bool): Whether to use Apache Arrow for optimized conversion.
            memory_fraction (float): Fraction of available memory to use for determining partitions.
            parquet_threshold (int): Size threshold in bytes for deciding when to use Parquet-based conversion.

        Returns:
            pd.DataFrame: The converted Pandas DataFrame.
        """
        conversion_method = self._determine_conversion_method(
            sdf, memory_fraction, parquet_threshold
        )

        if conversion_method == "toPandas":
            if use_arrow:
                self._logger.debug("Attempting conversion using Apache Arrow.")
                spark = sdf.sql_ctx.sparkSession
                spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
                try:
                    return sdf.toPandas()
                except Exception as e:
                    self._logger.error(
                        f"Arrow conversion failed: {e}, falling back to partition-based conversion."
                    )
            else:
                return sdf.toPandas()
        else:
            # Save as Parquet and read back using Pandas
            temp_parquet_path = "temp_dir/temp_data.parquet"
            os.makedirs(os.path.dirname(temp_parquet_path))
            self._logger.debug(
                f"Saving Spark DataFrame as Parquet to temporary file: {temp_parquet_path}"
            )
            sdf.write.mode("overwrite").parquet(temp_parquet_path)
            self._logger.debug("Reading Parquet data back into Pandas.")
            pdf = pd.read_parquet(temp_parquet_path)
            self._logger.debug("Removing temporary Parquet file.")
            shutil.rmtree(os.path.dirname(temp_parquet_path))
            return pdf

    def to_spark(self, pdf: pd.DataFrame, nlp: Optional[str] = None) -> DataFrame:
        """Convert a Pandas DataFrame to a Spark DataFrame with logging."""
        self._logger.debug("Starting Pandas to Spark DataFrame conversion.")
        spark_session = self._spark_session_pool.get_or_create(nlp=nlp)

        try:
            return spark_session.createDataFrame(pdf)
        except Exception as e:
            msg = f"Exception occurred while converting Pandas DataFrame to Spark DataFrame: {e}"
            self._logger.exception(msg)
            raise Exception(msg) from e
