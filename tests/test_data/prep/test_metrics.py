#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_data/prep/test_metrics.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 1st 2024 09:15:02 pm                                                  #
# Modified   : Tuesday June 4th 2024 12:10:40 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from appinsight.data_prep.metrics import (
    AppMetricsConfig,
    AppMetricsTask,
    AuthorMetricsConfig,
    AuthorMetricsTask,
    CategoryAuthorMetricsConfig,
    CategoryAuthorMetricsTask,
    CategoryMetricsConfig,
    CategoryMetricsTask,
    Metrics,
)
from appinsight.utils.io import PandasReader, PandasWriter

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.category
@pytest.mark.metrics
class TestMetricsCategory:  # pragma: no cover

    # ============================================================================================ #
    def test_metrics(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        config = CategoryMetricsConfig(force=True)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=CategoryMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_metrics_endpoint_exists(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = CategoryMetricsConfig(force=False)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=CategoryMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.app
@pytest.mark.metrics
class TestMetricsApp:  # pragma: no cover

    # ============================================================================================ #
    def test_metrics(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        config = AppMetricsConfig(force=True)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=AppMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_metrics_endpoint_exists(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = AppMetricsConfig(force=False)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=AppMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.author
@pytest.mark.metrics
class TestMetricsAuthor:  # pragma: no cover

    # ============================================================================================ #
    def test_metrics(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        config = AuthorMetricsConfig(force=True)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=AuthorMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_metrics_endpoint_exists(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = AuthorMetricsConfig(force=False)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=AuthorMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.catauth
@pytest.mark.metrics
class TestMetricsCategoryAuthor:  # pragma: no cover

    # ============================================================================================ #
    def test_metrics(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        config = CategoryAuthorMetricsConfig(force=True)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=CategoryAuthorMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_metrics_endpoint_exists(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = CategoryAuthorMetricsConfig(force=False)
        metrics = Metrics(
            config=config,
            spark=spark_session,
            metrics_task_cls=CategoryAuthorMetricsTask,
            source_reader_cls=PandasReader,
            target_writer_cls=PandasWriter,
            target_reader_cls=PandasReader,
        )
        df = metrics.execute()
        assert isinstance(df, pd.DataFrame)
        logger.info(df.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
