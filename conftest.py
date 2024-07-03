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
# Modified   : Wednesday July 3rd 2024 04:08:19 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import shutil
import sys

import pytest
from dotenv import load_dotenv

from appinsight.infrastructure.dependency.container import AppInsightContainer
from appinsight.infrastructure.persist.file.io import IOService

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
@pytest.fixture(scope="class", autouse=True)
def container():
    container = AppInsightContainer()
    container.init_resources()
    container.wire(
        packages=[
            "appinsight.infrastructure.persist.database",
            "appinsight.infrastructure.instrumentation",
        ],
        modules=["appinsight.infrastructure.persist.repo"],
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


# ------------------------------------------------------------------------------------------------ #
#                                 RESET TEST REPO                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="class", autouse=True)
def reset_repo(container):
    # Obtain database
    db = container.db.sqlite()

    # Drop table
    with open("scripts/sqlite3/dataset/drop.sql") as file:
        drop_query = file.read()
        print(f"\nExecuting drop query: {drop_query}")
        db.command(query=drop_query)

    # Verify table is dropped
    check_query = "SELECT * FROM sqlite_master WHERE type='table' AND name='dataset';"
    result = db.query(query=check_query)
    assert len(result) == 0, "Table 'datasets' should not exist after drop"

    # Recreate table
    with open("scripts/sqlite3/dataset/create.sql") as file:
        create_query = file.read()
        print(f"Executing create query: {create_query}")
        db.command(query=create_query)

    # Verify table is created
    result = db.query(query=check_query)
    assert len(result) == 1, "Table 'datasets' should exist after creation"

    # Delete files
    directory = "data/test"
    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory, exist_ok=True)

    print("Reset repository fixture executed")


# ------------------------------------------------------------------------------------------------ #
#                                       DATASET                                                    #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def reviews():
    DATASET_FP = "test/data/reviews"
    return IOService.read(filepath=DATASET_FP)
