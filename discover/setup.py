#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/setup.py                                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 06:28:52 am                                            #
# Modified   : Thursday January 23rd 2025 07:02:42 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import sys

from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.workspace.service import Workspace

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
@inject
def load_data(
    workspace: Workspace = Provide[DiscoverContainer.workspace.service],
):
    """Loads raw data into the workspace"""
    # Obtain and deserialize the raw dataset configuration
    # setup_config = AppConfigReader().get_config("setup", namespace=False)
    # dataset_config = ConfigDeserializer.deserialize_dataset_config(
    #     config=setup_config["dataset"]
    # )

    # TODO: Replace DatasetFactory with DatasetBuilder
    # Create dataset if it doesn't already exist.
    # asset_id = workspace.get_asset_id(**dataset_config)
    # if not workspace.repo.exists(asset_id=asset_id):
    #     dataset = DatasetFactory().from_parquet_file(**dataset_config)
    #     print(f"Dataset {dataset.asset_id} | {dataset.description} created.")


# ------------------------------------------------------------------------------------------------ #
def wire_container():
    container = DiscoverContainer()
    container.init_resources()
    container.wire(
        modules=[__name__],
        packages=["discover.flow", "discover.asset"],
    )
    return container


# ------------------------------------------------------------------------------------------------ #
def auto_wire_container():
    """Automatically wires the container if running in a notebook."""
    if "ipykernel" in sys.modules:
        return wire_container()
