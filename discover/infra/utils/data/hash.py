#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/data/hash.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday October 13th 2024 01:35:20 am                                                #
# Modified   : Sunday October 13th 2024 01:35:35 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import hashlib

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame


# ------------------------------------------------------------------------------------------------ #
class HashService:
    def __init__(self, num_rows: int = 5):
        """
        Initializes the HashService with the number of rows to consider for hashing.

        :param num_rows: Number of rows to sample from the DataFrame for hashing.
        """
        self.num_rows = num_rows

    def hash_dataframe(self, df) -> str:
        """
        Hashes the metadata of a DataFrame, supports both Pandas and Spark DataFrames.

        :param df: DataFrame (Pandas or Spark) to hash.
        :return: Hash string representing the DataFrame's metadata.
        """
        if isinstance(df, pd.DataFrame):
            return self._hash_pandas_dataframe(df)
        elif isinstance(df, SparkDataFrame):
            return self._hash_spark_dataframe(df)
        else:
            raise ValueError(
                "Unsupported DataFrame type. Must be Pandas or Spark DataFrame."
            )

    def _hash_pandas_dataframe(self, df: pd.DataFrame) -> str:
        """
        Generates a hash for a Pandas DataFrame based on its metadata.

        :param df: Pandas DataFrame to hash.
        :return: Hash string.
        """
        # Collect metadata
        metadata = (
            str(df.shape)
            + str(df.columns.tolist())
            + str(df.head(self.num_rows).values)
        )

        # Generate hash
        return hashlib.md5(metadata.encode("utf-8")).hexdigest()

    def _hash_spark_dataframe(self, df: SparkDataFrame) -> str:
        """
        Generates a hash for a Spark DataFrame based on its schema and first few rows.

        :param df: Spark DataFrame to hash.
        :return: Hash string.
        """
        # Collect metadata: schema and sample rows
        schema_str = str(df.schema)
        rows_str = str(df.limit(self.num_rows).collect())

        # Combine schema and rows metadata
        metadata = schema_str + rows_str

        # Generate hash
        return hashlib.md5(metadata.encode("utf-8")).hexdigest()
