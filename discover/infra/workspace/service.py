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
# Modified   : Tuesday January 21st 2025 06:28:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Type

from discover.asset.dataset.identity import DatasetPassport
from discover.asset.experiment.identity import ExperimentPassport
from discover.asset.model.identity import ModelPassport
from discover.core.dstruct import NestedNamespace
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.model import ModelRepo
from discover.infra.utils.file.fileset import FileFormat
from discover.infra.workspace.passport.idgen import IDGen
from discover.infra.workspace.passport.location import LocationService
from discover.infra.workspace.passport.version import VersionManager


# ------------------------------------------------------------------------------------------------ #
class Workspace:
    """Manages assets (datasets, models, experiments) within a project.

    This class provides methods for generating asset IDs, filepaths, and managing
    repositories for datasets, models, and experiments. It also handles versioning
    and location services.

    Attributes:
        _files (str): The base directory for files.
        _dataset_repo (DatasetRepo): The repository for datasets.
        _model_repo (ModelRepo): The repository for models.
        _experiment_repo (ExperimentRepo): The repository for experiments.
        _version_manager (VersionManager): Manages asset versioning.
        _location_service (LocationService): Provides file path resolution.
        _idgen (Type[IDGen]): The ID generator class.
        __passport_selector (Dict[str, Type[Passport]]): A dictionary mapping asset types to their passport classes.

    Properties:
        dataset_repo (DatasetRepo): Returns the dataset repository.
        model_repo (ModelRepo): Returns the model repository.
        experiment_repo (ExperimentRepo): Returns the experiment repository.
        tempdir (str): Returns the temporary directory path.

    Methods:
        get_version(phase, stage) -> str: Returns the next version ID for a given phase and stage.
        gen_asset_id(asset_type, phase, stage, name, version) -> str: Generates a unique asset ID.
        get_filepath(asset_id, phase, file_format) -> str: Generates a file path for an asset.
        _validate_config(config) -> NestedNamespace: Validates the workspace configuration.
    """

    __passport_selector = {
        "dataset": DatasetPassport,
        "model": ModelPassport,
        "experiment": ExperimentPassport,
    }

    def __init__(
        self,
        files: str,
        dataset_repo: DatasetRepo,
        model_repo: ModelRepo,
        experiment_repo: ExperimentRepo,
        version_manager: VersionManager,
        location_service: LocationService,
        idgen: Type[IDGen] = IDGen,
    ) -> None:
        """Initializes the Workspace.

        Args:
            files (str): The base directory for files.
            dataset_repo (DatasetRepo): The dataset repository.
            model_repo (ModelRepo): The model repository.
            experiment_repo (ExperimentRepo): The experiment repository.
            version_manager (VersionManager): Manages asset versioning.
            location_service (LocationService): Provides file path resolution.
            idgen (Type[IDGen]): The ID generator class.
        """
        self._files = files
        self._dataset_repo = dataset_repo
        self._model_repo = model_repo
        self._experiment_repo = experiment_repo
        self._version_manager = version_manager
        self._location_service = location_service
        self._idgen = idgen
        self._tempdir = None  # Initialize tempdir

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
    def tempdir(self) -> str:
        """Returns the temporary directory path."""
        return self._tempdir

    @tempdir.setter
    def tempdir(self, tempdir: str) -> None:
        """Sets the temporary directory path."""
        self._tempdir = tempdir

    def get_version(self, phase: PhaseDef, stage: StageDef) -> str:
        """Returns the next version ID for a given phase and stage.

        Args:
            phase (PhaseDef): The phase in which the asset is being created.
            stage (StageDef): The stage in which the asset is being created.

        Returns:
            str: The next version ID.
        """
        return self._version_manager.get_next_version(phase=phase, stage=stage)

    def gen_asset_id(
        self, asset_type: str, phase: PhaseDef, stage: StageDef, name: str, version: str
    ) -> str:
        """Generates a unique asset ID.

        Args:
            asset_type (str): The type of asset (e.g., "dataset", "model", "experiment").
            phase (PhaseDef): The phase in which the asset is being created.
            stage (StageDef): The stage in which the asset is being created.
            name (str): The asset's name.
            version (str): The asset's version ID.

        Returns:
            str: A unique asset identifier.
        """
        return self._idgen.gen_asset_id(
            asset_type=asset_type,
            phase=phase,
            stage=stage,
            name=name,
            version=version,
        )

    def get_filepath(
        self,
        asset_id: str,
        phase: PhaseDef,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> str:
        """Generates a file path for an asset.

        Args:
            asset_id (str): The ID of the asset.
            phase (PhaseDef): The phase associated with the asset.
            file_format (FileFormat): The format of the file (default is PARQUET).

        Returns:
            str: The file path for the asset.

        Raises:
            ValueError: If the file path resolution fails.
        """
        try:
            return self._location_service.get_filepath(
                asset_id=asset_id,
                phase=phase,
                file_format=file_format,
            )
        except Exception as e:
            raise ValueError(f"Failed to resolve the file path.\n{e}")

    def _validate_config(self, config) -> NestedNamespace:
        """Validates the workspace configuration.

        Args:
            config (dict): The configuration dictionary.

        Returns:
            NestedNamespace: A NestedNamespace object containing the validated configuration.

        Raises:
            KeyError: If required keys are missing from the configuration.
            ValueError: If configuration values are empty.
        """
        required_keys = ["metadata", "files", "ops"]
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise KeyError(f"Configuration is missing required keys: {missing_keys}")

        for key in required_keys:
            if not config[key]:
                raise ValueError(f"Configuration value for '{key}' must not be empty.")
        return NestedNamespace(config)
