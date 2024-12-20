#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/base.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 24th 2024 01:04:56 pm                                             #
# Modified   : Friday December 20th 2024 02:01:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Fileset Location Service Module"""

import os
from datetime import datetime

from discover.assets.base import Asset
from discover.assets.data.dataset import Dataset


# ------------------------------------------------------------------------------------------------ #
class Workspace:
    """Encapsulates data, models and experiments."""

    def __init__(self, config: dict) -> None:
        self._config = config

        self._location = config.get("workspace")
        self._file_location = os.path.join(self._location, config["data", "files"])
        self._idgen_db = os.path.join(self._location, config["data", "db", "idgen"])

    def register_dataset(self, dataset: Dataset) -> Dataset:
        """Assigns an asset_id and a filepath to the dataset

        Args:
            dataset (Dataset): Dataset object

        Returns:
            Dataset with asset_id, and filepath
        """
        dataset.asset_id = self._assign_asset_id(asset=dataset)
        filename = f"{dataset.asset_id}.parquet"
        dataset.filepath = os.path.join(self._file_location, filename)
        return dataset

    def _assign_asset_id(self, asset: Asset) -> Asset:
        """Assigns a unique asset_id to the asset and returns it.

        asset_id's have the form

        Args:
            asset (Asset): A Dataset, Model, or an Experiment asset.
        """
        try:
            asset_type = asset.__class__.__name__.lower()
            phase = asset.phase.value
            stage = asset.stage.value
            date = datetime.now().strftime("%Y%m%d")
            version = self._next_seq_num()
            version = str(version).zfill(4)
            return f"{asset_type}-{phase}-{stage}-{date}-{version}"
        except Exception as e:
            raise RuntimeError(f"Unable to generate an asset_id.\n{e}")
