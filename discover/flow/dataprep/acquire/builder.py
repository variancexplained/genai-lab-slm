#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/acquire/builder.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Wednesday January 1st 2025 05:43:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from typing import List, Optional

from dependency_injector.wire import Provide, inject

from discover.asset.dataset.dataset import Dataset
from discover.container import DiscoverContainer
from discover.flow.base.builder import StageBuilder
from discover.flow.dataprep.acquire.stage import AcquireStage
from discover.infra.workspace.service import Workspace


# ------------------------------------------------------------------------------------------------ #
class AcquireStageBuilder(StageBuilder):
    @inject
    def __init__(
        self, workspace: Workspace = Provide[DiscoverContainer.workspace.service]
    ) -> None:
        self._workspace = workspace
        self._source: Optional[Dataset] = None
        self._tasks: Optional[List] = None
        self._target: Optional[Dataset] = None

    @property
    def stage(self) -> AcquireStage:
        stage = self._stage
        self.reset()
        return stage

    def reset(self) -> None:
        self._source: Optional[Dataset] = None
        self._tasks: Optional[List] = None
        self._target: Optional[Dataset] = None

    def source(self, source: Dataset) -> AcquireStageBuilder:
        self._source = source

    def target(self, target: Dataset) -> AcquireStageBuilder:
        self._target = target

    def tasks(self, tasks: List) -> AcquireStageBuilder:
        self._tasks = tasks

    def build(self) -> AcquireStageBuilder:
        self._validate()
        self._stage = Stage(source=self._source, target=self._target, tasks=self._tasks)
        return self
