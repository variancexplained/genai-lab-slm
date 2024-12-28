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
# Modified   : Friday December 27th 2024 10:32:02 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from discover.asset.core import AssetType
from discover.asset.dataset import FileFormat
from discover.core.flow import PhaseDef, StageDef
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.model import ModelRepo


# ------------------------------------------------------------------------------------------------ #
class Workspace:
    """
    Represents a workspace for managing datasets, models, and experiments.

    The Workspace class provides a centralized interface to access and manage
    repositories for datasets, models, and experiments. It also handles file
    organization, temporary directories, and asset identification.

    Attributes:
        _config (dict): Configuration dictionary containing workspace details.
        _location (str): The base location of the workspace.
        _files (str): The directory where asset files are stored.
        _dataset_repo (DatasetRepo): Repository for managing datasets.
        _model_repo (ModelRepo): Repository for managing models.
        _experiment_repo (ExperimentRepo): Repository for managing experiments.

    Properties:
        dataset_repo (DatasetRepo): Access the dataset repository.
        model_repo (ModelRepo): Access the model repository.
        experiment_repo (ExperimentRepo): Access the experiment repository.
        location (str): The base location of the workspace.
        files (str): The directory for storing files.
        tempdir (str): Temporary directory for the workspace.

    Methods:
        get_asset_id(asset_type, phase, stage, name): Generates a unique asset ID.
        get_filepath(asset_id, file_format): Constructs a file path for an asset.
    """

    def __init__(
        self,
        config: dict,
        dataset_repo: DatasetRepo,
        model_repo: ModelRepo,
        experiment_repo: ExperimentRepo,
    ) -> None:
        """
        Initializes the Workspace object with configuration and repositories.

        Args:
            config (dict): Configuration dictionary containing workspace details such
                as location and file directories.
            dataset_repo (DatasetRepo): Repository instance for managing datasets.
            model_repo (ModelRepo): Repository instance for managing models.
            experiment_repo (ExperimentRepo): Repository instance for managing experiments.
        """
        self._config = config
        self._location = config["location"]
        self._files = os.path.join(self._location, config["files"])

        self._dataset_repo = dataset_repo
        self._model_repo = model_repo
        self._experiment_repo = experiment_repo

    @property
    def dataset_repo(self) -> DatasetRepo:
        """
        Access the dataset repository.

        Returns:
            DatasetRepo: The dataset repository instance.
        """
        return self._dataset_repo

    @property
    def model_repo(self) -> ModelRepo:
        """
        Access the model repository.

        Returns:
            ModelRepo: The model repository instance.
        """
        return self._model_repo

    @property
    def experiment_repo(self) -> ExperimentRepo:
        """
        Access the experiment repository.

        Returns:
            ExperimentRepo: The experiment repository instance.
        """
        return self._experiment_repo

    @property
    def location(self) -> str:
        """
        Access the base location of the workspace.

        Returns:
            str: The base location path.
        """
        return self._location

    @property
    def files(self) -> str:
        """
        Access the file storage directory.

        Returns:
            str: The file directory path.
        """
        return self._files

    @property
    def tempdir(self) -> str:
        """
        Access the temporary directory for the workspace.

        Returns:
            str: The temporary directory path.
        """
        return self._tempdir

    def get_asset_id(
        self,
        asset_type: AssetType,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
    ) -> str:
        """
        Generates a unique identifier for an asset.

        Args:
            asset_type (AssetType): The type of the asset (e.g., DATASET, MODEL).
            phase (PhaseDef): The lifecycle phase of the asset (e.g., TRAINING, EVALUATION).
            stage (StageDef): The processing stage of the asset (e.g., RAW, PROCESSED).
            name (str): A human-readable name for the asset.

        Returns:
            str: A unique asset identifier string.
        """
        return f"{asset_type.value}-{phase.directory}-{stage.directory}-{name}"

    def get_filepath(
        self, asset_id: str, file_format: FileFormat = FileFormat.PARQUET
    ) -> str:
        """
        Constructs the file path for a given asset.

        Args:
            asset_id (str): The unique identifier for the asset.
            file_format (FileFormat): The file format of the asset (e.g., PARQUET, CSV).
                Defaults to PARQUET.

        Returns:
            str: The full file path for the asset.
        """
        directory = self._files
        basename = asset_id
        filext = file_format.value
        filename = f"{basename}.{filext}"
        return os.path.join(directory, filename)
