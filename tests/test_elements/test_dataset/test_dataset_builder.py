#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_elements/test_dataset/test_dataset_builder.py                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 23rd 2024 02:12:36 am                                              #
# Modified   : Wednesday September 25th 2024 11:19:52 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import time
from datetime import datetime

import pandas as pd
import pyspark
import pytest

from discover.core.data_structure import DataStructure
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.dataset.build import DatasetBuilder
from discover.element.dataset.define import Dataset

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"

# ------------------------------------------------------------------------------------------------ #
NAME = "alex"
PHASE = PhaseDef.DATAPREP
STAGE = DataPrepStageDef.DQA
PARTITION_COLS = ["category"]
EXISTING_DATA_BEHAVIOR = "delete_matching"
MODE = "error"
RGS = 268435456


@pytest.mark.pandas
@pytest.mark.builder
@pytest.mark.dataset
class TestPandasDatasetBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_pandas_dataset_builder(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .data_structure(DataStructure.PANDAS)
            .not_partitioned()
            .stage(STAGE)
            .content(pandas_df)
            .build()
        )
        time.sleep(2)
        dataset.metadata.persist()
        assert isinstance(dataset, Dataset)

        # Validate storage config
        logging.info(dataset.storage_config)
        assert dataset.storage_config.data_structure == DataStructure.PANDAS
        assert dataset.storage_config.partitioned is False
        assert dataset.storage_config.filepath is not None
        assert dataset.storage_config.engine == "pyarrow"
        assert dataset.storage_config.compression == "snappy"
        assert dataset.storage_config.index is True
        assert dataset.storage_config.existing_data_behavior == "delete_matching"
        assert dataset.storage_config.row_group_size == 268435456
        assert dataset.storage_config.spark_session_name is None
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.mode is None
        assert dataset.storage_config.parquet_block_size is None

        # Validate metadata
        logging.info(dataset.metadata)
        assert dataset.metadata.id is not None
        assert dataset.metadata.name == NAME
        assert dataset.metadata.phase == PHASE
        assert dataset.metadata.stage == STAGE
        assert dataset.metadata.description is not None
        assert isinstance(dataset.metadata.created, datetime)
        assert isinstance(dataset.metadata.persisted, datetime)
        assert dataset.metadata.build_duration > 0
        assert dataset.metadata.element_type == "Dataset"
        assert dataset.metadata.nrows > 0
        assert dataset.metadata.ncols > 0
        assert dataset.metadata.size > 0

        assert isinstance(dataset.content, (pd.DataFrame, pd.core.frame.DataFrame))
        logging.info(dataset.content.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pandas_partitioned_dataset_builder(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .data_structure(DataStructure.PANDAS)
            .partitioned()
            .partition_cols(["category"])
            .stage(STAGE)
            .content(pandas_df)
            .build()
        )
        time.sleep(2)
        dataset.metadata.persist()
        assert isinstance(dataset, Dataset)

        # Validate storage config
        logging.info(dataset.storage_config)
        assert dataset.storage_config.data_structure == DataStructure.PANDAS
        assert dataset.storage_config.partitioned is True
        assert dataset.storage_config.partition_cols == ["category"]
        assert dataset.storage_config.filepath is not None
        assert dataset.storage_config.engine == "pyarrow"
        assert dataset.storage_config.compression == "snappy"
        assert dataset.storage_config.index is True
        assert dataset.storage_config.existing_data_behavior == "delete_matching"
        assert dataset.storage_config.row_group_size == 268435456
        assert dataset.storage_config.spark_session_name is None
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.mode is None
        assert dataset.storage_config.parquet_block_size is None

        # Validate metadata
        logging.info(dataset.metadata)
        assert dataset.metadata.id is not None
        assert dataset.metadata.name == NAME
        assert dataset.metadata.phase == PHASE
        assert dataset.metadata.stage == STAGE
        assert dataset.metadata.description is not None
        assert isinstance(dataset.metadata.created, datetime)
        assert isinstance(dataset.metadata.persisted, datetime)
        assert dataset.metadata.build_duration > 0
        assert dataset.metadata.element_type == "Dataset"
        assert dataset.metadata.nrows > 0
        assert dataset.metadata.ncols > 0
        assert dataset.metadata.size > 0

        assert isinstance(dataset.content, (pd.DataFrame, pd.core.frame.DataFrame))
        logging.info(dataset.content.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.spark
@pytest.mark.builder
@pytest.mark.dataset
class TestSparkDatasetBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_spark_dataset_builder(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .data_structure(DataStructure.SPARK)
            .not_partitioned()
            .stage(STAGE)
            .content(spark_df)
            .build()
        )
        time.sleep(2)
        dataset.metadata.persist()
        assert isinstance(dataset, Dataset)

        # Validate storage config
        logging.info(dataset.storage_config)
        assert dataset.storage_config.data_structure == DataStructure.SPARK
        assert dataset.storage_config.partitioned is False
        assert dataset.storage_config.filepath is not None
        assert dataset.storage_config.engine is None
        assert dataset.storage_config.compression is None
        assert dataset.storage_config.index is None
        assert dataset.storage_config.existing_data_behavior is None
        assert dataset.storage_config.row_group_size is None
        assert dataset.storage_config.spark_session_name == "leviathan"
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.mode == "error"
        assert dataset.storage_config.parquet_block_size is not None

        # Validate metadata
        logging.info(dataset.metadata)
        assert dataset.metadata.id is not None
        assert dataset.metadata.name == NAME
        assert dataset.metadata.phase == PHASE
        assert dataset.metadata.stage == STAGE
        assert dataset.metadata.description is not None
        assert isinstance(dataset.metadata.created, datetime)
        assert isinstance(dataset.metadata.persisted, datetime)
        assert dataset.metadata.build_duration > 0
        assert dataset.metadata.element_type == "Dataset"
        assert dataset.metadata.nrows > 0
        assert dataset.metadata.ncols > 0
        assert dataset.metadata.size > 0

        assert isinstance(dataset.content, pyspark.sql.DataFrame)
        logging.info(dataset.content.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_partitioned_dataset_builder(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .partitioned()
            .data_structure(DataStructure.SPARK)
            .partition_cols(["category"])
            .stage(STAGE)
            .content(spark_df)
            .build()
        )
        time.sleep(2)
        dataset.metadata.persist()
        assert isinstance(dataset, Dataset)

        # Validate storage config
        logging.info(dataset.storage_config)
        assert dataset.storage_config.data_structure == DataStructure.SPARK
        assert dataset.storage_config.partitioned is True
        assert dataset.storage_config.partition_cols == ["category"]
        assert dataset.storage_config.filepath is not None
        assert dataset.storage_config.engine is None
        assert dataset.storage_config.compression is None
        assert dataset.storage_config.index is None
        assert dataset.storage_config.existing_data_behavior is None
        assert dataset.storage_config.row_group_size is None
        assert dataset.storage_config.spark_session_name == "leviathan"
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.mode == "error"
        assert dataset.storage_config.parquet_block_size is not None

        # Validate metadata
        logging.info(dataset.metadata)
        assert dataset.metadata.id is not None
        assert dataset.metadata.name == NAME
        assert dataset.metadata.phase == PHASE
        assert dataset.metadata.stage == STAGE
        assert dataset.metadata.description is not None
        assert isinstance(dataset.metadata.created, datetime)
        assert isinstance(dataset.metadata.persisted, datetime)
        assert dataset.metadata.build_duration > 0
        assert dataset.metadata.element_type == "Dataset"
        assert dataset.metadata.nrows > 0
        assert dataset.metadata.ncols > 0
        assert dataset.metadata.size > 0

        assert isinstance(dataset.content, pyspark.sql.DataFrame)
        logging.info(dataset.content.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
