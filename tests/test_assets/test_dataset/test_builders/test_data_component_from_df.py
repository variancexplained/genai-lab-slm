#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_builders/test_data_component_from_df.py        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 06:16:36 pm                                               #
# Modified   : Tuesday December 31st 2024 03:11:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import DataFrame

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import DataComponentBuilderFromDataFrame
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


@pytest.mark.data
@pytest.mark.pandas
@pytest.mark.builder
@pytest.mark.dataset
class TestDatasetBuilderFromPandasDataFrame:  # pragma: no cover
    # ============================================================================================ #
    def test_builder_pandas_df_csv(self, ds_passport, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromDataFrame()
        dc = (
            builder.dataframe(pandas_df)
            .passport(ds_passport)
            .to_csv()
            .build()
            .data_component
        )

        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.PANDAS
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.CSV
        assert isinstance(dc.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))

        logging.info(dc)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_builder_pandas_df_parquet(self, ds_passport, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromDataFrame()
        dc = (
            builder.dataframe(pandas_df)
            .passport(ds_passport)
            .to_parquet()
            .build()
            .data_component
        )

        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.PANDAS
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.PARQUET
        assert isinstance(dc.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))

        logging.info(dc)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(TypeError):
            builder = DataComponentBuilderFromDataFrame()
            _ = builder.dataframe(pandas_df).passport(2).to_csv().build().data_component

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_type(self, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(TypeError):
            builder = DataComponentBuilderFromDataFrame()
            _ = (
                builder.dataframe(2)
                .passport(ds_passport)
                .to_csv()
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
    def test_validate_file_meta(self, ds_passport, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(FileNotFoundError):
            builder = DataComponentBuilderFromDataFrame()
            dc = (
                builder.dataframe(pandas_df)
                .passport(ds_passport)
                .to_csv()
                .build()
                .data_component
            )
            _ = dc.file_meta

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.data
@pytest.mark.spark
@pytest.mark.builder
@pytest.mark.dataset
class TestDatasetBuilderFromSparkDataFrame:  # pragma: no cover
    # ============================================================================================ #
    def test_builder_spark_df(self, ds_passport, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromDataFrame()
        dc = (
            builder.dataframe(spark_df)
            .passport(ds_passport)
            .to_csv()
            .build()
            .data_component
        )

        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.CSV
        assert isinstance(dc.dataframe, DataFrame)

        logging.info(dc)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_builder_spark_df(self, ds_passport, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromDataFrame()
        dc = (
            builder.dataframe(spark_df)
            .passport(ds_passport)
            .to_parquet()
            .build()
            .data_component
        )

        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.SPARK
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.PARQUET
        assert isinstance(dc.dataframe, DataFrame)

        logging.info(dc)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(TypeError):
            builder = DataComponentBuilderFromDataFrame()
            _ = builder.dataframe(spark_df).passport(2).to_csv().build().data_component

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_type(self, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(TypeError):
            builder = DataComponentBuilderFromDataFrame()
            _ = (
                builder.dataframe(2)
                .passport(ds_passport)
                .to_csv()
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
    def test_validate_file_meta(self, ds_passport, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(FileNotFoundError):
            builder = DataComponentBuilderFromDataFrame()
            dc = (
                builder.dataframe(spark_df)
                .passport(ds_passport)
                .to_csv()
                .build()
                .data_component
            )
            _ = dc.file_meta

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
