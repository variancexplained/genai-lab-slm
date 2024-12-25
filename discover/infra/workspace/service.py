#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/service.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 11:31:34 am                                               #
# Modified   : Wednesday December 25th 2024 12:14:32 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from discover.asset.base import Asset
from discover.core.asset import AssetType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.model import ModelRepo


# ------------------------------------------------------------------------------------------------ #
class WorkspaceService:

    def __init__(
        self,
        config: dict,
        dataset_repo: DatasetRepo,
        model_repo: ModelRepo,
        experiment_repo: ExperimentRepo,
    ) -> None:
        self._config = config
        self._location = config["location"]
        self._files = os.path.join(self._location, config["files"])
        self._dataset_repo = dataset_repo
        self._model_repo = model_repo
        self._experiment_repo = experiment_repo

    @property
    def dataset_repo(self) -> DatasetRepo:
        return self._dataset_repo

    @property
    def model_repo(self) -> ModelRepo:
        return self._model_repo

    @property
    def experiment_repo(self) -> ExperimentRepo:
        return self._experiment_repo

    @property
    def location(self) -> str:
        return self._location

    @property
    def files(self) -> str:
        return self._files

    def get_asset_id(
        self,
        asset_type: AssetType,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        **kwargs,
    ) -> str:
        return f"{asset_type.value}-{phase.value}-{stage.value}-{name}"

    def set_asset_id(self, asset: Asset) -> Asset:
        asset_id = self.get_asset_id(
            asset_type=asset.asset_type,
            phase=asset.phase,
            stage=asset.stage,
            name=asset.name,
        )
        setattr(asset, "_asset_id", asset_id)
        return asset

    def set_filepath(self, asset: Asset) -> Asset:
        if asset.asset_id is None:
            asset = self.set_asset_id(asset=asset)
        directory = self._files
        basename = asset.asset_id
        filext = asset.file_format.value
        filename = f"{basename}.{filext}"
        filepath = os.path.join(directory, filename)
        setattr(asset, "_filepath", filepath)
        return asset
