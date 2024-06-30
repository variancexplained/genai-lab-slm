#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /conftest.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday April 25th 2024 12:55:55 am                                                #
# Modified   : Sunday June 30th 2024 05:06:55 am                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import random
import sys
from datetime import datetime

import pytest
from dotenv import load_dotenv
from pyspark.sql.types import DoubleType, LongType, StringType, TimestampNTZType

from appinsight.container import AppInsightContainer
from appinsight.infrastructure.file.io import IOService
from appinsight.infrastructure.profiling.profile import TaskProfile
from appinsight.infrastructure.frameworks.spark.factory import SparkSessionFactory

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = [
    "data/**/*.*",
    "tests/test_analysis/*.*",
    "tests/test_data/*.*",
    "tests/test_infrastructure/*.*",
]
# ------------------------------------------------------------------------------------------------ #
# pylint: disable=redefined-outer-name, no-member
# ------------------------------------------------------------------------------------------------ #
TEXT_COLUMN = "content"


# ------------------------------------------------------------------------------------------------ #
#                                     DATA FIXTURE - RAW                                           #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def data_raw():
    """Returns raw test data."""
    FP = "data/test/00_raw/reviews.pkl"
    df = IOService.read(filepath=FP)
    return df


# ------------------------------------------------------------------------------------------------ #
#                                 DATA FIXTURE - NORMALIZED                                        #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def data_norm():
    """Returns normalized test data."""
    FP = "data/test/01_normalized/reviews.pkl"
    df = IOService.read(filepath=FP)
    return df


# ------------------------------------------------------------------------------------------------ #
#                                   DATA FIXTURE - DQA                                             #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def data_dqa():
    """Returns normalized test data."""
    FP = "data/test/02_dqa/reviews.pkl"
    df = IOService.read(filepath=FP)
    return df


# ------------------------------------------------------------------------------------------------ #
#                                   DATA FIXTURE - CLEAN                                           #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def data_clean():
    """Returns clean test data."""
    FP = "data/test/03_clean/reviews.pkl"
    df = IOService.read(filepath=FP)
    return df


# ------------------------------------------------------------------------------------------------ #
#                                    SPARK SESSION                                                 #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def spark_session():
    # Set up spark session
    factory = SparkSessionFactory()
    spark = factory.build(name="AppInsight Test", nlp=False)
    yield spark

    # Tear down
    spark.stop()


# ------------------------------------------------------------------------------------------------ #
#                                  SPARK NLP SESSION                                               #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def spark_nlp_session():
    # Set up spark session
    factory = SparkSessionFactory()
    spark = factory.build(name="AppInsight Test", nlp=True)
    yield spark

    # Tear down
    spark.stop()


# ------------------------------------------------------------------------------------------------ #
#                              DATA FIXTURE - SPARK DATAFRAME                                      #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def spark_dataframe(spark_session):
    fp = "tests/data/reviews.parquet"
    return spark_session.read.load(
        fp, format="parquet", inferSchema="true", header="true"
    )


# ------------------------------------------------------------------------------------------------ #
#                                   PYSPARK SCHEMA                                                 #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def pyspark_schema():
    return {
        "id": StringType(),
        "app_id": StringType(),
        "app_name": StringType(),
        "category_id": StringType(),
        "category": StringType(),
        "author": StringType(),
        "rating": DoubleType(),
        "title": StringType(),
        "content": StringType(),
        "vote_count": LongType(),
        "vote_sum": LongType(),
        "date": TimestampNTZType(),
    }


# ------------------------------------------------------------------------------------------------ #
#                                   PANDAS SCHEMA                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def pandas_schema():
    return {
        "id": "string",
        "app_id": "string",
        "app_name": "string",
        "category_id": "category",
        "category": "category",
        "author": "string",
        "rating": "int64",
        "title": "string",
        "content": "string",
        "vote_count": "int64",
        "vote_sum": "int64",
        "date": "datetime64[ms]",
    }


# ------------------------------------------------------------------------------------------------ #
#                                      PROFILE                                                     #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=False)
def profile():
    """Returns a profile object."""
    return TaskProfile(
        task="test_task",
        stage="test_stage",
        phase="testing",
        runtime=random.uniform(1, 1000),
        cpu_usage_pct=random.random(),
        memory_usage=random.uniform(1, 1000),
        disk_read_bytes=random.randint(100, 100000),
        disk_write_bytes=random.randint(100, 100000),
        disk_total_bytes=random.randint(100, 100000),
        input_records=random.randint(100, 100000),
        output_records=random.randint(100, 100000),
        input_records_per_second=random.uniform(1, 1000),
        output_records_per_second=random.uniform(1, 1000),
        timestamp=datetime.timestamp(datetime.now()),
    )


# ------------------------------------------------------------------------------------------------ #
#                              DEPENDENCY INJECTION                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="module", autouse=True)
def container():
    container = AppInsightContainer()
    container.init_resources()
    container.wire(
        packages=[
            "appinsight.infrastructure.database",
            "appinsight.infrastructure.profiling",
        ]
    )

    return container


# ------------------------------------------------------------------------------------------------ #
#                                 CHECK ENVIRONMENT                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session", autouse=True)
def check_environment():
    # Get the current environment
    load_dotenv()
    current_env = os.environ.get("ENV")

    # Check if the current environment is 'test'
    if current_env != "test":
        print(
            "Tests can only be run in the 'test' environment. Current environment is: {}".format(
                current_env
            )
        )
        sys.exit(1)
