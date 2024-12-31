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
# Modified   : Tuesday December 31st 2024 02:51:47 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Type

from discover.asset.base.atype import AssetType
from discover.asset.dataset import FileFormat
from discover.core.data_structure import NestedNamespace
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.model import ModelRepo
from discover.infra.workspace.idgen import IDGen
from discover.infra.workspace.location import LocationService
from discover.infra.workspace.version import VersionManager


# ------------------------------------------------------------------------------------------------ #
class Workspace:

    def __init__(
        self,
        dataset_repo: DatasetRepo,
        model_repo: ModelRepo,
        experiment_repo: ExperimentRepo,
        version_manager: VersionManager,
        location_service: LocationService,
        idgen: Type[IDGen] = IDGen,
    ) -> None:
        self._dataset_repo = dataset_repo
        self._model_repo = model_repo
        self._exepriment_repo = experiment_repo
        self._version_manager = version_manager
        self._location_service = location_service
        self._idgen = idgen

    @property
    def dataset_repo(self) -> DatasetRepo:
        """Returns the dataset repository."""
        return self._dataset_repo

    @property
    def model_repo(self) -> ModelRepo:
        """Returns the model repository."""
        return self._model_repo

    @property
    def experiment_repo(self) -> ExperimentRepo:
        """Returns the experiment repository."""
        return self._experiment_repo

    @property
    def location(self) -> str:
        """Returns the base location of the workspace."""
        return self._location

    @property
    def files(self) -> str:
        """Returns the filesets associated with the workspace."""
        return self._files

    @property
    def tempdir(self) -> str:
        """Returns the temporary directory path."""
        return self._tempdir

    def gen_asset_id(
        self,
        asset_type: AssetType,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
    ) -> str:
        """
        Generates a unique asset ID.

        Args:
            asset_type (AssetType): The type of the asset.
            phase (PhaseDef): The phase associated with the asset.
            stage (StageDef): The stage associated with the asset.
            name (str): The name of the asset.

        Returns:
            str: A unique asset ID.
        """
        return self._idgen.gen_asset_id(
            asset_type=asset_type, phase=phase, stage=stage, name=name
        )

    def get_filepath(
        self,
        asset_type: AssetType,
        asset_id: str,
        phase: PhaseDef,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> str:
        """Generates a filepath for the given asset_type, asset_id, phase, and file format.

        Args:
            asset_type (AssetType): The type of the asset.
            asset_id (str): The ID of the asset.
            phase (PhaseDef): The phase associated with the asset.
            file_format (FileFormat): The format of the file (default is PARQUET).

        Returns:
            str: The file path for the asset.

        Raises:
            ValueError: If the file path resolution fails.
        """
        filepath = self._location_service.get_filepath(
            asset_type=asset_type,
            asset_id=asset_id,
            phase=phase,
            file_format=file_format,
        )
        if not filepath:
            raise ValueError("Failed to resolve the file path.")
        return filepath

    def _validate_config(self, config) -> NestedNamespace:
        required_keys = ["metadata", "files", "ops"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise KeyError(f"Configuration is missing required keys: {missing_keys}")

        for key in required_keys:
            if not config[key]:
                raise ValueError(f"Configuration value for '{key}' must not be empty.")
        return NestedNamespace(config)
