#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /conftest.py                                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday April 25th 2024 12:55:55 am                                                #
# Modified   : Monday September 16th 2024 12:27:25 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import sys

import pandas as pd
import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from discover.application.service.data.ingest import DataIngestionServiceConfig
from discover.container import DiscoverContainer
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.reader import ConfigReader
from discover.infra.database.schema import schema
from discover.infra.repo.mckinney import McKinneyRepo
from discover.infra.storage.cloud.aws import S3Handler

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = []
# ------------------------------------------------------------------------------------------------ #
# pylint: disable=redefined-outer-name, no-member
# ------------------------------------------------------------------------------------------------ #


# ------------------------------------------------------------------------------------------------ #
#                              DEPENDENCY INJECTION                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session", autouse=True)
def container() -> DiscoverContainer:
    container = DiscoverContainer()
    container.init_resources()
    container.wire(
        packages=[],
    )

    return container


# ------------------------------------------------------------------------------------------------ #
#                                 CHECK ENVIRONMENT                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session", autouse=True)
def check_environment() -> None:
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


# ------------------------------------------------------------------------------------------------ #
#                                     DATABASE SETUP                                               #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session", autouse=True)
def db_setup(container) -> None:
    dba = container.db.admin()
    dba.drop_table(tablename="profile")
    dba.create_table(schema=schema["profile"])


# ------------------------------------------------------------------------------------------------ #
#                                      PROFILE REPO                                                #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def profile_repo(container):
    return container.repo.profile


# ------------------------------------------------------------------------------------------------ #
#                                         AWS                                                      #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def aws():
    return S3Handler(config_reader_cls=ConfigReader)


# ------------------------------------------------------------------------------------------------ #
#                                        SPARK                                                     #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture to create a Spark session.
    This fixture is session-scoped, meaning it will be created once per test session.
    """
    spark_session = (
        SparkSession.builder.appName("pytest-spark-session")
        .master("local[*]")
        .getOrCreate()
    )

    yield spark_session

    # Teardown after the test session ends
    spark_session.stop()


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture
def pandas_df():
    """
    Pytest fixture that reads a CSV file into a pandas DataFrame.
    Modify this to point to the correct CSV file.
    """
    FILEPATH = "data/test/00_raw/reviews.csv"
    return pd.read_csv(FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture
def spark_df(spark, pandas_df):
    """
    Pytest fixture that converts a pandas DataFrame to a Spark DataFrame.
    Requires the spark fixture and pandas_df_from_csv fixture.
    """

    return spark.createDataFrame(pandas_df)


# ------------------------------------------------------------------------------------------------ #
#                                         CONFIG                                                   #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def data_ingestion_service_config():
    source = DataConfig(repo=McKinneyRepo(), stage=Stage.RAW, name="reviews")
    target = DataConfig(repo=McKinneyRepo(), stage=Stage.INGEST, name="reviews")
    return DataIngestionServiceConfig(
        source_data_config=source, target_data_config=target
    )
