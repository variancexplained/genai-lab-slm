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
# Modified   : Monday December 23rd 2024 08:23:39 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.persist.repo.experiment import ExperimentRepo
from discover.infra.persist.repo.inference import InferenceRepo
from discover.infra.persist.repo.model import ModelRepo


# ------------------------------------------------------------------------------------------------ #
class WorkspaceService:
    @inject
    def __init__(
        self,
        config: str = Provide[DiscoverContainer.config.workspace],
        dataset_repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        model_repo: ModelRepo = Provide[DiscoverContainer.repo.model_repo],
        inference_repo: InferenceRepo = Provide[DiscoverContainer.repo.inference_repo],
        experiment_repo: ExperimentRepo = Provide[
            DiscoverContainer.repo.experiment_repo
        ],
    ) -> None:
        self._config = config
        self._location = config["location"]
        self._files = os.path.join(self._location, config["files"])
        self._dataset_repo = dataset_repo
        self._model_repo = model_repo
        self._inference_repo = inference_repo
        self._experiment_repo = experiment_repo

    @property
    def dataset_repo(self) -> DatasetRepo:
        return self._dataset_repo

    @property
    def model_repo(self) -> ModelRepo:
        return self._model_repo

    @property
    def inference_repo(self) -> InferenceRepo:
        return self._inference_repo

    @property
    def experiment_repo(self) -> ExperimentRepo:
        return self._experiment_repo

    @property
    def location(self) -> str:
        return self._location

    @property
    def files(self) -> str:
        return self._files
