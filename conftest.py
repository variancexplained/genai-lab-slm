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
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import sys

import pytest
from appvocai.setup.file.config import FileSetupPipelineConfig
from appvocai.shared.dependency.container import AppInsightContainer
from appvocai.shared.persist.file.io import IOService
from dotenv import load_dotenv

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
# ------------------------------------------------------------------------------------------------ #
collect_ignore_glob = ["test/infrastructure/*.*"]
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
            "appinsight.shared.persist.database",
            "appinsight.shared.instrumentation",
        ],
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
#                                       DATASET                                                    #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def reviews():
    DATASET_FP = "test/data/reviews"
    return IOService.read(filepath=DATASET_FP)


# ------------------------------------------------------------------------------------------------ #
#                               FILE SETUP CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
@pytest.fixture(scope="session")
def file_setup_config():
    return FileSetupPipelineConfig(
        local_download_folder="test/data",
        extract_destination="test/data/reviews",
        aws_folder="test",
        aws_s3_key="reviews.tar.gz",
        frac=0.1,
        force=True,
    )
