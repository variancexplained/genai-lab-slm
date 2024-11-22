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
# Modified   : Thursday November 21st 2024 10:31:02 pm                                             #
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
from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.data_prep.base import DataPrepStage
from discover.infra.config.app import AppConfigReader
from discover.infra.config.flow import FlowConfigReader
from discover.infra.persistence.cloud.aws import S3Handler
from discover.infra.utils.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = [
    "tests/test_infra/test_dal/**/*py",
    "tests/test_infra/test_operations/**/*py",
    "tests/test_infra/test_storage/**/*py",
    "tests/test_orchestration/**/*py",
]
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
            "discover.flow.stage.base",
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
    return S3Handler(config_reader_cls=AppConfigReader)


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
        stage=StageDef.TQA,
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
        stage=StageDef.TQA,
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
        stage=StageDef.TQA,
        content=spark_df,
    )
    repo = container.repo.dataset_repo()
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)
    yield dataset
    repo.remove(asset_id=dataset.asset_id, ignore_errors=True)


# ------------------------------------------------------------------------------------------------ #
#                                  DATA PREP                                                       #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def norm_asset_id(container):
    reader = FlowConfigReader()
    config = reader.get_config("phases", namespace=False)
    stage_config = config["dataprep"]["stages"][1]

    # Build Stage
    stage = DataPrepStage.build(stage_config=stage_config)
    asset_id = stage.run()
    return asset_id
