#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/orchestration/pipeline.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 03:42:28 am                                                   #
# Modified   : Monday September 9th 2024 09:41:24 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Union

import pandas as pd
from appvocai.shared.utils.datetime import convert_seconds_to_hms
from appvocai.shared.utils.print import Printer
from appvocai.utils.io import FileReader, FileWriter
from appvocai.utils.repo import ReviewRepo
from pyspark.sql import DataFrame

# ------------------------------------------------------------------------------------------------ #
#                                       STAGE CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #


@dataclass
class StageConfig(ABC):
    """Abstract base class for data preprocessing stage configurations."""

    name: str = None
    source_directory: str = None
    source_filename: str = None
    target_directory: str = None
    target_filename: str = None
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
#                                        PIPELINE                                                  #
# ------------------------------------------------------------------------------------------------ #
class Pipeline:
    """Pipeline to manage and execute a sequence of tasks."""

    def __init__(self):
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

    def run(self):
        """Execute the pipeline tasks in sequence."""

        self._start_pipeline()

        data = None

        for task in self._tasks:

            data = task.run(data)
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


# ------------------------------------------------------------------------------------------------ #
#                                      PREPROCESSOR                                                #
# ------------------------------------------------------------------------------------------------ #
class PipelineBuilder(ABC):
    """Abstract base class for data preprocessor classes

    Args:
        config (StageConfig): Configuration for the subclass stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
    """

    def __init__(
        self,
        config: StageConfig,
        source_reader_cls: type[Reader] = FileReader,
        target_writer_cls: type[Writer] = FileWriter,
        target_reader_cls: type[Reader] = FileReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
        **kwargs,
    ) -> None:
        super().__init__()
        self.config = config
        self.source_reader_cls = source_reader_cls
        self.target_writer_cls = target_writer_cls
        self.target_reader_cls = target_reader_cls
        self.pipeline_cls = pipeline_cls
        self.review_repo = review_repo_cls()
        self._data = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def data(self) -> Union[pd.DataFrame, DataFrame]:
        return self._data

    @abstractmethod
    def create_pipeline(self) -> Pipeline:
        """Constructs the pipeline that executes the preprocessing tasks."""

    def run(self) -> Union[pd.DataFrame, DataFrame]:
        """Executes the preprocessing tasks.

        The pipeline runs if the endpoint doesn't already exist or if
        the config.force is True. If the endpoint already exists and the
        config.force is False, the endpoint is read and returned.
        """
        self.logger.debug("Checking if endpoint exists.")
        if self.endpoint_exists() and not self.config.force:
            self.logger.info("Endpoint exists. Returning data from endpoint.")
            self._data = self.read_endpoint()
        else:
            self.logger.debug("Creating pipeline")
            pipeline = self.create_pipeline()
            self.logger.debug("Pipeline created.")
            self._data = pipeline.run()
        return self._data

    def endpoint_exists(self) -> bool:
        """Returns True if the target already exists. False otherwise"""

        try:
            return self.review_repo.exists(
                directory=self.config.target_directory,
                filename=self.config.target_filename,
            )
        except FileNotFoundError:
            return False

    def read_endpoint(self) -> Union[pd.DataFrame, DataFrame]:
        """Reads and returns the target data."""
        filepath = self.review_repo.get_filepath(
            directory=self.config.target_directory, filename=self.config.target_filename
        )
        try:
            data = self.target_reader_cls().read(filepath=filepath)
            msg = (
                f"{self.config.name} endpoint already exists. Returning prior results."
            )
            self.logger.debug(msg)
            return data
        except Exception as e:
            msg = f"Exception occurred while reading endpoint at {filepath}.\n{e}"
            self.logger.exception(msg)
            raise
