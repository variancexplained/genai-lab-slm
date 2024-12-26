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
# Modified   : Wednesday December 25th 2024 07:44:43 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import sys

from dependency_injector.wiring import Provide, inject

from discover.asset.dataset import DatasetFactory
from discover.container import DiscoverContainer
from discover.flow.stage.base import deserialize_dataset_config
from discover.infra.config.app import AppConfigReader
from discover.infra.workspace.service import WorkspaceService

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
@inject
def load_data(
    workspace_service: WorkspaceService = Provide[DiscoverContainer.workspace.service],
):
    """Loads raw data into the workspace"""
    # Obtain and deserialize the raw dataset configuration
    setup_config = AppConfigReader().get_config("setup", namespace=False)
    dataset_config = deserialize_dataset_config(setup_config["dataset"])

    # Create dataset if it doesn't already exist.
    asset_id = workspace_service.get_asset_id(**dataset_config)
    if not workspace_service.dataset_repo.exists(asset_id=asset_id):
        dataset = DatasetFactory().from_parquet_file(**dataset_config)
        print(f"Dataset {dataset.asset_id} | {dataset.description} created.")


# ------------------------------------------------------------------------------------------------ #
def wire_container():
    container = DiscoverContainer()
    container.init_resources()
    container.wire(
        modules=["discover.asset.dataset", "discover.flow.stage.base", __name__]
    )
    return container


# ------------------------------------------------------------------------------------------------ #
def auto_wire_container():
    """Automatically wires the container if running in a notebook."""
    if "ipykernel" in sys.modules:
        return wire_container()
