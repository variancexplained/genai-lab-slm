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
# Modified   : Tuesday September 24th 2024 08:44:52 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.base.store import StorageConfig
from discover.element.dataset.build import (
    PandasDatasetBuilder,
    PandasPartitionedDatasetBuilder,
    SparkDatasetBuilder,
    SparkPartitionedDatasetBuilder,
)
from discover.element.dataset.define import (
    PandasDataset,
    PandasPartitionedDataset,
    SparkDataset,
    SparkPartitionedDataset,
)

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
NAME = "test_dataset"
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
        builder = PandasDatasetBuilder()
        dataset = (
            builder.name(NAME).phase(PHASE).stage(STAGE).content(pandas_df).build()
        )
        assert isinstance(dataset, PandasDataset)
        logging.info(dataset)
        assert isinstance(dataset.id, int)
        assert dataset.name == NAME
        assert dataset.phase == PHASE
        assert dataset.stage == STAGE
        assert dataset.content.equals(pandas_df)
        assert isinstance(dataset.storage_config, StorageConfig)
        assert dataset.storage_config.partitioned is False
        assert dataset.storage_config.write_kwargs["compression"] == "snappy"
        assert dataset.storage_config.write_kwargs["index"] is False
        assert dataset.storage_config.write_kwargs["engine"] == "pyarrow"
        assert dataset.storage_config.row_group_size == RGS
        logging.info(dataset.storage_config)

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
        builder = PandasPartitionedDatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .stage(STAGE)
            .partition_cols(PARTITION_COLS)
            .existing_data_behavior(EXISTING_DATA_BEHAVIOR)
            .content(pandas_df)
            .build()
        )
        assert isinstance(dataset, PandasPartitionedDataset)
        logging.info(dataset)
        assert isinstance(dataset.id, int)
        assert dataset.name == NAME
        assert dataset.phase == PHASE
        assert dataset.stage == STAGE
        assert dataset.content.equals(pandas_df)
        assert isinstance(dataset.storage_config, StorageConfig)
        assert dataset.storage_config.partitioned is True
        assert dataset.storage_config.write_kwargs["compression"] == "snappy"
        assert dataset.storage_config.write_kwargs["index"] is False
        assert dataset.storage_config.write_kwargs["engine"] == "pyarrow"
        assert dataset.storage_config.write_kwargs["partition_cols"] == ["category"]
        assert (
            dataset.storage_config.write_kwargs["existing_data_behavior"]
            == "delete_matching"
        )
        assert dataset.storage_config.row_group_size == RGS

        logging.info(dataset.storage_config)

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
        builder = SparkDatasetBuilder()
        dataset = builder.name(NAME).phase(PHASE).stage(STAGE).content(spark_df).build()
        assert isinstance(dataset, SparkDataset)
        logging.info(dataset)
        assert isinstance(dataset.id, int)
        assert dataset.name == NAME
        assert dataset.phase == PHASE
        assert dataset.stage == STAGE
        logging.info(
            f"Spark DataFrame comparison: {dataset.content.exceptAll(spark_df)}"
        )
        assert isinstance(dataset.storage_config, StorageConfig)
        assert dataset.storage_config.partitioned is False
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.row_group_size == RGS
        assert dataset.storage_config.spark_session_name == "leviathan"

        logging.info(dataset.storage_config)

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
        builder = SparkPartitionedDatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .stage(STAGE)
            .partition_cols(PARTITION_COLS)
            .content(spark_df)
            .build()
        )
        assert isinstance(dataset, SparkPartitionedDataset)
        logging.info(dataset)
        assert isinstance(dataset.id, int)
        assert dataset.name == NAME
        assert dataset.phase == PHASE
        assert dataset.stage == STAGE
        logging.info(
            f"Spark DataFrame comparison: {dataset.content.exceptAll(spark_df)}"
        )
        assert isinstance(dataset.storage_config, StorageConfig)
        assert dataset.storage_config.partitioned is True
        assert dataset.storage_config.nlp is False
        assert dataset.storage_config.write_kwargs["mode"] == "error"
        assert dataset.storage_config.write_kwargs["partition_cols"] == ["category"]
        assert dataset.storage_config.spark_session_name == "leviathan"

        logging.info(dataset.storage_config)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_nlp_dataset_builder(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = SparkPartitionedDatasetBuilder()
        dataset = (
            builder.name(NAME)
            .phase(PHASE)
            .stage(STAGE)
            .nlp()
            .partition_cols(PARTITION_COLS)
            .content(spark_df)
            .build()
        )
        assert isinstance(dataset, SparkPartitionedDataset)
        logging.info(dataset)
        assert isinstance(dataset.id, int)
        assert dataset.name == NAME
        assert dataset.phase == PHASE
        assert dataset.stage == STAGE
        logging.info(
            f"Spark DataFrame comparison: {dataset.content.exceptAll(spark_df)}"
        )
        assert isinstance(dataset.storage_config, StorageConfig)
        assert dataset.storage_config.partitioned is True
        assert dataset.storage_config.write_kwargs["mode"] == "error"
        assert dataset.storage_config.write_kwargs["partition_cols"] == ["category"]
        assert dataset.storage_config.spark_session_name == "leviathan"
        assert dataset.storage_config.nlp is True

        logging.info(dataset.storage_config)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
