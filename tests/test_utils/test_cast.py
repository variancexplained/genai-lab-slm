#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_utils/test_cast.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 04:58:00 am                                                 #
# Modified   : Sunday June 30th 2024 03:10:20 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from appinsight.utils.cast import CastPandas, CastPySpark
from appinsight.utils.convert import ToSpark
from appinsight.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.cast
@pytest.mark.cast_pandas
class TestCast:  # pragma: no cover
    # ============================================================================================ #
    def test_pandas_cast(self, data_raw, pandas_schema):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        DIRECTORY = "02_cast"
        FILENAME = "cast_reviews.csv"
        review_repo = ReviewRepo()
        review_repo.write(data=data_raw, directory=DIRECTORY, filename=FILENAME)
        df1 = review_repo.read(directory=DIRECTORY, filename=FILENAME)
        cast = CastPandas()
        df2 = cast.apply(data=df1, datatypes=pandas_schema)
        logging.info(df2.info())
        for column, datatype in pandas_schema.items():
            assert df2[column].dtype == datatype

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pyspark_cast(self, spark_dataframe, pyspark_schema):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        cast = CastPySpark()
        logging.info(spark_dataframe.printSchema())
        with pytest.raises(Exception):
            df = cast.apply(data=spark_dataframe, datatypes=pyspark_schema)
            logger.info(df.printSchema())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
