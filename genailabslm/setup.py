#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/setup.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 06:28:52 am                                            #
# Modified   : Saturday January 25th 2025 05:50:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import sys
from typing import Type

from dependency_injector.wiring import inject
from genailabslm.asset.dataset.builder import DatasetBuilder
from genailabslm.asset.dataset.config import DatasetConfig
from genailabslm.container import GenAILabSLMContainer
from genailabslm.core.dtypes import DFType
from genailabslm.infra.config.app import AppConfigReader
from genailabslm.infra.utils.file.fileset import FileFormat

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
@inject
def load_data(
    container: GenAILabSLMContainer,
    config_reader_cls: Type[AppConfigReader] = AppConfigReader,
):
    """Reads the data, creates a Dataset object, and loads it into the repository"""
    # Obtain repository, file access, and spark dependencies.
    repo = container.io.repo()
    fao = container.io.fao()
    spark_session_pool = container.spark.session_pool()

    # Get the configuration reader
    config_reader = config_reader_cls()
    # Get the current environment from configuration
    env = config_reader.get_environment()
    print(f"GenAILab Data Load for the {env.upper()} Environment Started")
    # Obtain the data setup configuration
    config = config_reader.get_config(section="setup", namespace=False)["data"]
    # Create the configuration for the dataset to be loaded into the workspace
    dataset_config = DatasetConfig.from_dict(config["dataset_config"])
    # Check existence and proceed with load if the dataset doesn't already exist
    asset_id = repo.get_asset_id(
        phase=dataset_config.phase, stage=dataset_config.stage, name=dataset_config.name
    )
    if not repo.exists(asset_id=asset_id):
        # Obtain a spark session which will be used to read the source data
        spark = spark_session_pool.spark
        # Load the PySpark DataFrame
        dataframe = fao.read(
            filepath=config["source_filepath"],
            dftype=DFType.SPARK,
            file_format=FileFormat.PARQUET,
            spark=spark,
        )
        # Construct a Dataset object
        dataset = (
            DatasetBuilder()
            .from_config(config=dataset_config)
            .dataframe(dataframe)
            .creator("AppVoCAI")
            .build()
        )
        # Persist the dataset in the repository
        dataset = repo.add(dataset=dataset, entity="GenAILabSetup")
    else:
        print(f"Dataset {asset_id} for {env.upper()} Workspace Exists.")
    assert repo.exists(asset_id=asset_id)
    print(
        f"AppVoCAI Dataset {asset_id} Loaded into the GenAILab {env.upper()} Workspace!"
    )


# ------------------------------------------------------------------------------------------------ #
def wire_container():
    container = GenAILabSLMContainer()
    container.init_resources()
    container.wire(
        modules=[__name__],
        packages=[
            "genailabslm.flow.dataprep",
            "genailabslm.flow.base",
            "genailabslm.asset",
        ],
    )
    return container


# ------------------------------------------------------------------------------------------------ #
def auto_wire_container():
    """Automatically wires the container if running in a notebook."""
    if "ipykernel" in sys.modules:
        return wire_container()
