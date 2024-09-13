#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_monitor/test_profiler.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 11th 2024 03:58:14 pm                                           #
# Modified   : Friday September 13th 2024 12:01:34 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import random
import time
from datetime import datetime

import pandas as pd
import pytest

from discover.domain.service.base.task import Task
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.monitor.profiler import task_profiler

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


# ------------------------------------------------------------------------------------------------ #
class TestTask(Task):
    def __init__(self):
        super().__init__(stage=Stage.CORE)

    @task_profiler
    def run(self):
        time.sleep(random.random())


@pytest.mark.profiler
class TestProfiler:  # pragma: no cover
    # ============================================================================================ #
    def test_profiler(self, profile_repo, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = TestTask()
        for i in range(5):
            task.run()

        repo = profile_repo()
        metrics = repo.get_all()
        assert isinstance(metrics, pd.DataFrame)
        assert "TestTask" in metrics["task_name"].values
        logger.info(metrics)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
