#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/base/stage.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 03:43:30 am                                              #
# Modified   : Thursday January 2nd 2025 09:38:50 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod
from typing import List

from discover.archive.flow.task.base import Task
from discover.asset.base.identity import Passport
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.passport import DatasetPassport
from discover.infra.persist.object.flowstate import FlowState


# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    def __init__(
        self,
        source: DatasetPassport,
        tasks: List[Task],
        target: DatasetPassport,
        flow_state: FlowState,
    ) -> None:
        self._source = source
        self._tasks = tasks
        self._target = target
        self._flow_state = flow_state

    @abstractmethod
    def passport(self) -> Passport:
        """Returns the passport for the stage."""

    def run(self) -> Dataset:
        # Step 1: Initialize the flow state using FlowManager
        source_passport = self._flow_state.read()

        # Step 2: Execute all tasks sequentially
        data = self._source.data.dataframe
        for task in self._tasks:
            try:
                data = task.run(
                    data
                )  # Task execution doesn't directly interact with FlowManager
            except Exception as e:
                # You can handle task errors here or let them propagate
                print(f"Error in task {task.__class__.__name__}: {e}")
                break

        # Step 3: Update the target dataset with the final processed data
        self._target.data.dataframe = data

        # Step 4: Finalize the flow state using FlowManager
        self._state.write(passport=self._target.passport)

        return self._target
