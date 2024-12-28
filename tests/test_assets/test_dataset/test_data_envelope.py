#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_data_envelope.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 28th 2024 12:40:58 am                                             #
# Modified   : Saturday December 28th 2024 12:50:26 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import DataEnvelopeBuilder
from discover.asset.dataset.component.data import DataEnvelope

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.envelope
@pytest.mark.builder
class TestDataEnvelopeBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_builder_pandas_parquet(self, dataset_builder, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataEnvelopeBuilder(dataset_builder=dataset_builder)
        envelope = (
            builder.pandas()
            .parquet()
            .data(pandas_df)
            .filepath("test/data_envelope/pandas.parquet")
            .build()
            .envelope
        )
        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.dftype, DFType)
        assert isinstance(envelope.file_format, FileFormat)
        assert isinstance(envelope.filepath, str)
        assert envelope.file_format == FileFormat.PARQ
        assert envelope.dftype == DFType.PANDAS
        assert isinstance(envelope.data, (pd.DataFrame, pd.core.frame.DataFrame))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_builder_spark_parquet(self, dataset_builder, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataEnvelopeBuilder(dataset_builder=dataset_builder)
        envelope = (
            builder.spark()
            .parquet()
            .data(spark_df)
            .filepath("test/data_envelope/spark.parquet")
            .build()
            .envelope
        )
        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.dftype, DFType)
        assert isinstance(envelope.file_format, FileFormat)
        assert isinstance(envelope.filepath, str)
        assert envelope.file_format == FileFormat.PARQ
        assert envelope.dftype == DFType.SPARK
        assert isinstance(envelope.data, (pd.DataFrame, pd.core.frame.DataFrame))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )

    # ============================================================================================ #
    def test_builder_sparknlp_csv(self, dataset_builder, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataEnvelopeBuilder(dataset_builder=dataset_builder)
        envelope = (
            builder.sparknlp()
            .csv()
            .data(spark_df)
            .filepath("test/data_envelope/spark.parquet")
            .build()
            .envelope
        )
        assert isinstance(envelope, DataEnvelope)
        assert isinstance(envelope.dftype, DFType)
        assert isinstance(envelope.file_format, FileFormat)
        assert isinstance(envelope.filepath, str)
        assert envelope.file_format == FileFormat.PARQ
        assert envelope.dftype == DFType.SPARKNLP
        assert isinstance(envelope.data, (pd.DataFrame, pd.core.frame.DataFrame))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
