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
# Modified   : Thursday October 17th 2024 12:22:08 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import sys

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from discover.assets.dataset import Dataset
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.infra.config.reader import ConfigReader
from discover.infra.persistence.cloud.aws import S3Handler
from discover.infra.utils.file.io import IOService

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
        modules=[
            "discover.infra.utils.file.io",
            "discover.infra.persistence.dal.fao.distributed",
        ],
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
    # Assuming the log4j.properties file is in the root directory
    log4j_conf_path = "file:" + os.path.abspath("log4j.properties")
    spark_session = (
        SparkSession.builder.appName("pytest-spark-session")
        .master("local[*]")
        .config(
            "spark.driver.extraJavaOptions", f"-Dlog4j.configuration={log4j_conf_path}"
        )
        .config(
            "spark.executor.extraJavaOptions",
            f"-Dlog4j.configuration={log4j_conf_path}",
        )
        .getOrCreate()
    )
    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    # Teardown after the test session ends
    spark_session.stop()


# ------------------------------------------------------------------------------------------------ #
#                                       DATA                                                       #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def pandas_df():
    """
    Pytest fixture that reads a CSV file into a pandas DataFrame.
    Modify this to point to the correct CSV file.
    """
    FILEPATH = "data/working/reviews"
    return IOService.read(filepath=FILEPATH)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark_df(spark, pandas_df):
    """
    Pytest fixture that converts a pandas DataFrame to a Spark DataFrame.
    Requires the spark fixture and pandas_df_from_csv fixture.
    """

    return spark.createDataFrame(pandas_df)


# ------------------------------------------------------------------------------------------------ #
#                                  DATASETS                                                        #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def centralized_ds(pandas_df, container):
    dataset = Dataset(
        name="test_centralized_ds",
        nlp=False,
        distributed=False,
        phase=PhaseDef.DATAPREP,
        stage=DataPrepStageDef.DQA,
        content=pandas_df,
    )
    repo = container.repo.dataset_repo()
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)
    yield dataset
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def distributed_ds(spark_df, container):
    dataset = Dataset(
        name="test_distributed_ds",
        nlp=False,
        distributed=True,
        phase=PhaseDef.DATAPREP,
        stage=DataPrepStageDef.DQA,
        content=spark_df,
    )
    repo = container.repo.dataset_repo()
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)
    yield dataset
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def distributed_ds_nlp(spark_df, container):
    dataset = Dataset(
        name="test_distributed_nlp_ds",
        nlp=True,
        distributed=True,
        phase=PhaseDef.DATAPREP,
        stage=DataPrepStageDef.DQA,
        content=spark_df,
    )
    repo = container.repo.dataset_repo()
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)
    yield dataset
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)
