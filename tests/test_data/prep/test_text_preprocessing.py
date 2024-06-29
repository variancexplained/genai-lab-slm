#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_data/prep/test_text_preprocessing.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 1st 2024 09:15:02 pm                                                  #
# Modified   : Friday June 28th 2024 06:50:20 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest
from pyspark.sql import DataFrame

from appinsight.data_prep.text_prep import TextPrepConfig, TextPreprocessor
from appinsight.utils.io import FileReader, PySparkReader, PySparkWriter

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.text
class TestTextPreprocessingTask:  # pragma: no cover

    # ============================================================================================ #
    def test_text(self, spark_nlp_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        config = TextPrepConfig(force=True)
        prep = TextPreprocessor(
            config=config,
            spark=spark_nlp_session,
            source_reader_cls=FileReader,
            target_writer_cls=PySparkWriter,
            target_reader_cls=PySparkReader,
        )
        df = prep.execute()
        assert isinstance(df, DataFrame)
        logger.info(df.select("*").show())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_text_endpoint_exists(self, spark_session, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = TextPrepConfig(force=False)
        prep = TextPreprocessor(
            config=config,
            spark=spark_session,
            source_reader_cls=FileReader,
            target_writer_cls=PySparkWriter,
            target_reader_cls=PySparkReader,
        )
        df = prep.execute()
        assert isinstance(df, DataFrame)
        logger.info(df.select("*").show())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
