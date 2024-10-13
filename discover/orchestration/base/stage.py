#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/base/stage.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 08:14:05 pm                                              #
# Modified   : Sunday October 13th 2024 01:58:08 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Base Class  Module"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import List, Union

import pandas as pd
import pyspark
from dependency_injector.wiring import Provide

from discover.assets.dataset import Dataset
from discover.container import DiscoverContainer
from discover.core.flow import PhaseDef, StageDef
from discover.core.namespace import NestedNamespace
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.orchestration.base.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                        STAGE                                                     #
# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    def __init__(
        self,
        phase: str,
        stage: str,
        source: str,  # Source dataset name
        tasks: List[Task],
        nlp: bool = False,  # Whether this stage is for NLP
        distributed: bool = False,  # Whether this stage produces a distributed dataset
        force: bool = False,  # Whether to force execution if endpoint exists.
        repo: DatasetRepo = Provide[
            DiscoverContainer.repo.dataset_repo
        ],  # Dataset persistence
        **kwargs,
    ) -> None:
        self._phase = PhaseDef.from_value(phase)
        self._stage = StageDef.from_value(stage)
        self._source = source
        self._tasks = tasks
        self._nlp = nlp
        self._distributed = distributed
        self._force = force
        self._repo = repo

        self._dataset = None
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        return self._phase

    @property
    def stage(self) -> StageDef:
        return self._stage

    @property
    def source_config(self) -> NestedNamespace:
        return self._source_config

    @property
    def destination_config(self) -> NestedNamespace:
        return self._destination_config

    @property
    def force(self) -> bool:
        return self._force

    @property
    def logger(self) -> logging.Logger:
        """
        Provides read-only access to the logger instance for this pipeline.

        Returns:
        --------
        logging.Logger:
            The logger instance associated with this pipeline.
        """
        return self._logger

    @abstractmethod
    def setup(self) -> None:
        """Stage setup operations"""

    @abstractmethod
    def run(self) -> None:
        """Stage execution"""

    @abstractmethod
    def teardown(self) -> None:
        """Stage teardown operations"""

    def load_source_dataset(self) -> None:
        self._dataset = self._repo.get(name=self._source)

    def create_destination_dataset(
        self, content: Union[pd.DataFrame, pyspark.sql.DataFrame]
    ) -> Dataset:
        return Dataset(
            phase=self._phase,
            stage=self._stage,
            content=content,
            nlp=self._nlp,
            distributed=self._distributed,
        )

    def save_destination_dataset(self, dataset: Dataset) -> None:
        self._repo.add(dataset=dataset)

    def endpoint_exists(self) -> bool:
        """Checks existence of the data endpoint."""
        return self._repo.exists(name=self._source)
