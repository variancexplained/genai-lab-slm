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
# Modified   : Friday October 11th 2024 12:33:52 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import sys

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.dataset import Dataset
from discover.infra.config.reader import ConfigReader
from discover.infra.database.schema import schema
from discover.infra.repo.config import (
    CentralizedDatasetStorageConfig,
    DistributedDatasetStorageConfig,
)
from discover.infra.repo.dataset import DatasetRepo
from discover.infra.storage.cloud.aws import S3Handler
from discover.infra.storage.local.io import IOService

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
            "discover.infra.storage.local.io",
            "discover.infra.dal.file.distributed",
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
    FILEPATH = "workspace/test/00_raw/reviews"
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
def pandas_ds(pandas_df):
    storage_config = CentralizedDatasetStorageConfig(partitioned=False)
    dataset = Dataset(
        phase=PhaseDef.DATAPREP,
        stage=DataPrepStageDef.DQA,
        content=pandas_df,
        storage_config=storage_config,
    )
    repo = DatasetRepo()
    if repo.exists(name=dataset.name):
        repo.remove(name=dataset.name)
    yield dataset
    repo.remove(name=dataset.name)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def pandas_partitioned_ds(pandas_df):
    storage_config = CentralizedDatasetStorageConfig()
    dataset = Dataset(
        phase=PhaseDef.DATAPREP,
        stage=DataPrepStageDef.NORM,
        content=pandas_df,
        storage_config=storage_config,
    )
    repo = DatasetRepo()
    if repo.exists(name=dataset.name):
        repo.remove(name=dataset.name)
    yield dataset
    repo.remove(name=dataset.name)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def spark_ds(spark_df):
    storage_config = DistributedDatasetStorageConfig(partitioned=False)
    dataset = Dataset(
        phase=PhaseDef.FEATURE,
        stage=DataPrepStageDef.CLEAN,
        content=spark_df,
        storage_config=storage_config,
    )
    repo = DatasetRepo()
    if repo.exists(name=dataset.name):
        repo.remove(name=dataset.name)
    yield dataset
    repo.remove(name=dataset.name)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def spark_partitioned_ds(spark_df):
    storage_config = DistributedDatasetStorageConfig()
    dataset = Dataset(
        phase=PhaseDef.FEATURE,
        stage=DataPrepStageDef.DQA,
        content=spark_df,
        storage_config=storage_config,
    )
    repo = DatasetRepo()
    if repo.exists(name=dataset.name):
        repo.remove(name=dataset.name)
    yield dataset
    repo.remove(name=dataset.name)


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="function")
def spark_partitioned_nlp_ds(spark_df):
    storage_config = DistributedDatasetStorageConfig(nlp=True)
    dataset = Dataset(
        phase=PhaseDef.FEATURE,
        stage=DataPrepStageDef.CLEAN,
        content=spark_df,
        storage_config=storage_config,
    )
    repo = DatasetRepo()
    if repo.exists(name=dataset.name):
        repo.remove(name=dataset.name)
    yield dataset
    repo.remove(name=dataset.name)
