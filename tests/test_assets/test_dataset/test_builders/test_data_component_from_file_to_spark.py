#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_builders/test_data_component_from_file_to_spark.py #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday December 31st 2024 10:32:48 pm                                              #
# Modified   : Wednesday January 1st 2025 01:37:54 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import DataComponentBuilderFromFile
from discover.asset.dataset.component.data import DataComponent
from discover.asset.dataset.component.identity import DatasetPassport

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
CSV_FILEPATH = "tests/data/reviews.csv"
PARQUET_FILEPATH = "tests/data/reviews"
BOGUS_FILEPATH = "tests/data/bogus"


@pytest.mark.dcfile
@pytest.mark.dcfile_spark
class TestSparkDCFromCSVFile:  # pragma: no cover
    # ============================================================================================ #
    def test_build(self, workspace, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        dc = (
            builder.passport(ds_passport)
            .source_filepath(CSV_FILEPATH)
            .spark(spark)
            .to_spark()
            .build()
            .data_component
        )

        assert isinstance(dc, DataComponent)
        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.CSV
        assert isinstance(dc.dataframe, DataFrame)
        logging.info(dc.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_spark_session(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Should provide a spark session
        dc = (
            builder.passport(ds_passport)
            .source_filepath(CSV_FILEPATH)
            .from_csv()
            .to_spark()
            .build()
            .data_component
        )

        assert isinstance(dc, DataComponent)
        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.CSV
        assert isinstance(dc.dataframe, DataFrame)
        logging.info(dc.dataframe.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_source_file(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test validation for source file
        with pytest.raises(TypeError):
            _ = (
                builder.passport(ds_passport)
                .from_csv()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test validation for passport
        with pytest.raises(TypeError):
            _ = (
                builder.source_filepath(CSV_FILEPATH)
                .from_csv()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_file_not_found(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test file not found error
        with pytest.raises(FileNotFoundError):
            _ = (
                builder.passport(ds_passport)
                .source_filepath(BOGUS_FILEPATH)
                .from_csv()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dcfile
@pytest.mark.dcfile_spark
class TestSparkDCFromPARQUETFile:  # pragma: no cover
    # ============================================================================================ #
    def test_build(self, workspace, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        dc = (
            builder.passport(ds_passport)
            .source_filepath(PARQUET_FILEPATH)
            .from_parquet()
            .to_spark()
            .spark(spark)
            .build()
            .data_component
        )

        assert isinstance(dc, DataComponent)
        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.PARQUET
        assert isinstance(dc.dataframe, DataFrame)
        logging.info(dc.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_spark_session(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Should provide a spark session
        dc = (
            builder.passport(ds_passport)
            .source_filepath(PARQUET_FILEPATH)
            .from_parquet()
            .to_spark()
            .build()
            .data_component
        )

        assert isinstance(dc, DataComponent)
        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.PARQUET
        assert isinstance(dc.dataframe, DataFrame)
        logging.info(dc.dataframe.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_source_file(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test validation for source file
        with pytest.raises(TypeError):
            _ = (
                builder.passport(ds_passport)
                .from_parquet()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test validation for passport
        with pytest.raises(TypeError):
            _ = (
                builder.source_filepath(PARQUET_FILEPATH)
                .from_parquet()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_file_not_found(self, ds_passport, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromFile()
        # Test file not found error
        with pytest.raises(FileNotFoundError):
            _ = (
                builder.passport(ds_passport)
                .source_filepath(BOGUS_FILEPATH)
                .from_parquet()
                .spark(spark)
                .to_spark()
                .build()
                .data_component
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
