#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/manager.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 04:53:26 am                                              #
# Modified   : Thursday January 2nd 2025 10:33:38 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from discover.archive.flow.task.base import TaskBuilder
from discover.asset.dataset.builder import DatasetBuilder
from discover.infra.persist.object.flowstate import FlowState


class FlowManager:
    def __init__(
        self,
        flowstate: FlowState,
        dataset_builder: DatasetBuilder,
        task_builder: TaskBuilder,
    ) -> None:
        self._flowstate = flowstate
        self._dataset_builder = dataset_builder
        self._task_builder = task_builder
