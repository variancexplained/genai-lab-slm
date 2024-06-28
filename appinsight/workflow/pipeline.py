#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/workflow/pipeline.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 06:44:30 am                                                 #
# Modified   : Thursday June 27th 2024 04:31:33 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from datetime import datetime

import pandas as pd

from appinsight.utils.datetime import convert_seconds_to_hms
from appinsight.utils.print import Printer
from appinsight.workflow.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline:
    """Pipeline to manage and execute a sequence of tasks."""

    def __init__(self, name: str):
        self._name = name
        self._pipeline_start = None
        self._pipeline_stop = None
        self._pipeline_runtime = None
        self._data = None
        self._tasks = []
        self._tasks_completed = []
        self._summary = None
        self._task_summary = None

        self._printer = Printer()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def name(self) -> str:
        return self._name

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @property
    def summary(self) -> pd.DataFrame:
        return self._summary

    @property
    def task_summary(self) -> pd.DataFrame:
        return self._task_summary

    def add_task(self, task: Task):
        """Add a task to the pipeline.

        Args:
            task (Task): A task object to add to the pipeline.
        """
        self._tasks.append(task)

    def execute(self):
        """Execute the pipeline tasks in sequence."""

        self._start_pipeline()

        data = None

        for task in self._tasks:

            data = task.execute(data)
            self._data = data if data is not None else self._data

            self._tasks_completed.append(task)

        self._stop_pipeline()
        return self._data

    def _start_pipeline(self) -> None:
        self._printer.print_header(title=f"{self._name} Pipeline")
        self._pipeline_start = datetime.now()

    def _stop_pipeline(self) -> None:
        self._pipeline_stop = datetime.now()
        self._pipeline_runtime = convert_seconds_to_hms(
            (self._pipeline_stop - self._pipeline_start).total_seconds()
        )

        self._task_summary = self._capture_metrics()

        self._summary = {
            "Pipeline Start": self._pipeline_start,
            "Pipeline Stop": self._pipeline_stop,
            "Pipeline Runtime": self._pipeline_runtime,
        }
        self._printer.print_dict(title=self.name, data=self._summary)
        self._printer.print_trailer()

    def _capture_metrics(self) -> pd.DataFrame:
        metrics = []
        for task in self._tasks_completed:
            metrics.append(task.metrics)
        return pd.DataFrame(metrics)
