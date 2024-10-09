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
# Modified   : Tuesday October 8th 2024 10:18:24 pm                                                #
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
from discover.core.data_structure import DataStructure
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.dataset.build import DatasetBuilder
from discover.infra.config.reader import ConfigReader
from discover.infra.database.schema import schema
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
            "discover.element.base.build",
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
    FILEPATH = "workspace/test/00_dataprep/01_ingest/reviews"
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
@pytest.fixture(scope="session")
def pandas_ds(pandas_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("pandas")
        .phase(PhaseDef.DATAPREP)
        .data_structure(DataStructure.PANDAS)
        .not_partitioned()
        .stage(DataPrepStageDef.DQA)
        .content(pandas_df)
        .build()
    )
    dataset.storage_config.filepath = (
        "workspace/test/00_dataprep/01_ingest/reviews.parquet"
    )
    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def pandas_partitioned_ds(pandas_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("pandas_partitioned")
        .phase(PhaseDef.DATAPREP)
        .data_structure(DataStructure.PANDAS)
        .partitioned()
        .partition_cols("category")
        .stage(DataPrepStageDef.RAW)
        .content(pandas_df)
        .build()
    )
    dataset.storage_config.filepath = "workspace/test/00_dataprep/01_ingest/reviews"
    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark_ds(spark_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("spark")
        .phase(PhaseDef.TRANSFORMATION)
        .data_structure(DataStructure.SPARK)
        .not_partitioned()
        .stage(DataPrepStageDef.DQA)
        .content(spark_df)
        .build()
    )
    dataset.storage_config.filepath = "workspace/test/00_dataprep/99_test/dataprep_test_test_spark_storage_dataset_20240924-003.parquet"
    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark_partitioned_ds(spark_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("spark_partitioned")
        .phase(PhaseDef.TRANSFORMATION)
        .data_structure(DataStructure.SPARK)
        .partitioned()
        .partition_cols("category")
        .stage(DataPrepStageDef.RAW)
        .content(spark_df)
        .build()
    )
    dataset.storage_config.filepath = "workspace/test/00_dataprep/99_test/dataprep_test_test_spark_partitioned_storage_dataset_20240924-004"
    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark_nlp_ds(spark_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("spark_nlp")
        .phase(PhaseDef.DATAPREP)
        .data_structure(DataStructure.SPARK)
        .not_partitioned()
        .nlp()
        .stage(DataPrepStageDef.RAW)
        .content(spark_df)
        .build()
    )
    dataset.storage_config.filepath = "workspace/test/00_dataprep/99_test/dataprep_test_test_spark_nlp_storage_dataset_20240924-005.parquet"
    return dataset


# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def spark_partitioned_nlp_ds(spark_df):
    builder = DatasetBuilder()
    dataset = (
        builder.name("spark_partitioned_nlp")
        .phase(PhaseDef.DATAPREP)
        .data_structure(DataStructure.SPARK)
        .partitioned()
        .nlp()
        .partition_cols("category")
        .stage(DataPrepStageDef.RAW)
        .content(spark_df)
        .build()
    )

    dataset.storage_config.filepath = "workspace/test/00_dataprep/99_test/dataprep_test_test_spark_nlp_partitioned_storage_dataset_20240924-006"
    return dataset
