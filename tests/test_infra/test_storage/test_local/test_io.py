#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_storage/test_local/test_io.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 04:28:20 pm                                            #
# Modified   : Monday September 23rd 2024 07:27:16 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
from datetime import datetime

import pandas as pd
import pytest

from discover.element.dataset.store import (
    PandasParquetDatasetStorageConfig,
    SparkParquetDatasetStorageConfig,
)
from discover.infra.storage.local.io import IOService

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"

OFP_PANDAS = "tests/data/infra/storage/local/pandas.parquet"
OFP_PANDAS_PART = "tests/data/infra/storage/local/pandas_partitioned"
OFP_SPARK = "tests/data/infra/storage/local/spark.parquet"
OFP_SPARK_PART = "tests/data/infra/storage/local/spark_partitioned"


@pytest.mark.io
class TestIOService:  # pragma: no cover
    # ============================================================================================ #
    def test_parquet_pandas(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain a storage configuration object.
        config = PandasParquetDatasetStorageConfig(partitioned=False)
        # Instantiate the IOService and write the file, passing the storage config object.
        io = IOService()
        io.write(filepath=OFP_PANDAS, data=pandas_df, storage_config=config)
        # Confirm the file has been created
        assert os.path.exists(OFP_PANDAS)
        # Read the data and confirm data type and equality to input.
        df = io.read(filepath=OFP_PANDAS)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))
        assert df.equals(pandas_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_parquet_pandas_partitioned(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain a storage configuration object.
        config = PandasParquetDatasetStorageConfig(partitioned=True)
        # Instantiate the IOService and write the file, passing the storage config object.
        io = IOService()
        io.write(filepath=OFP_PANDAS_PART, data=pandas_df, storage_config=config)
        # Confirm the file has been created
        assert os.path.exists(OFP_PANDAS_PART)
        # Confirm the filepath is a directory
        assert os.path.isdir(OFP_PANDAS_PART)
        # Read the data and confirm data type and equality to input.
        df = io.read(filepath=OFP_PANDAS_PART)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))
        assert df.equals(pandas_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_parquet_spark(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain a storage configuration object.
        config = SparkParquetDatasetStorageConfig(partitioned=False)
        # Instantiate the IOService and write the file, passing the storage config object.
        io = IOService()
        io.write(filepath=OFP_SPARK, data=spark_df, storage_config=config)
        # Confirm the file has been created
        assert os.path.exists(OFP_SPARK)
        # Read the data and confirm data type and equality to input.
        df = io.read(filepath=OFP_SPARK)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))
        assert df.equals(spark_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_parquet_spark_partitioned(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain a storage configuration object.
        config = SparkParquetDatasetStorageConfig(partitioned=True)
        # Instantiate the IOService and write the file, passing the storage config object.
        io = IOService()
        io.write(filepath=OFP_SPARK_PART, data=spark_df, storage_config=config)
        # Confirm the file has been created
        assert os.path.exists(OFP_SPARK_PART)
        # Confirm the filepath is a directory
        assert os.path.isdir(OFP_SPARK_PART)
        # Read the data and confirm data type and equality to input.
        df = io.read(filepath=OFP_SPARK_PART)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))
        assert df.equals(spark_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
