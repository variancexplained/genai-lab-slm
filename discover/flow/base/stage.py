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
# Modified   : Thursday January 2nd 2025 07:52:52 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC
from typing import List

import pandas as pd
from git import Union
from pyspark.sql import DataFrame

from discover.archive.flow.task.base import Task
from discover.asset.dataset.dataset import Dataset
from discover.infra.persist.object.flowstate import FlowState
from discover.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class Stage(ABC):
    """
    Abstract base class for executing a stage in a data processing pipeline.

    A `Stage` represents a transformation process where a `source` dataset is processed
    through a series of `tasks`, resulting in a `target` dataset. The class also handles
    metadata management and persistence of datasets via a repository and flow state.

    Args:
        source (Dataset): The input dataset to be processed.
        tasks (List[Task]): A list of tasks to execute sequentially on the source dataset.
        target (Dataset): The output dataset to store the results after processing.
        state (FlowState): The flow state for managing and persisting metadata (e.g., passports).
        repo (DatasetRepo): Repository for storing the resulting dataset.

    Attributes:
        _source (Dataset): The input dataset that will be processed in the stage.
        _tasks (List[Task]): The list of tasks that define the processing steps.
        _target (Dataset): The output dataset that will hold the final processed data.
        _state (FlowState): The flow state for managing and persisting metadata such as passports.
        _repo (DatasetRepo): The repository used to persist the target dataset.
        _logger (Logger): Logger instance for capturing execution details and errors.

    Methods:
        run() -> Dataset:
            Executes the processing pipeline by applying all tasks to the source dataset,
            updating the target dataset, and persisting both the dataset and metadata.
    """

    def __init__(
        self,
        source: Dataset,
        tasks: List[Task],
        target: Dataset,
        state: FlowState,
        repo: DatasetRepo,
    ) -> None:
        """Initializes the Stage with source, tasks, target, state, and repository."""
        self._source = source
        self._tasks = tasks
        self._target = target
        self._state = state
        self._repo = repo

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def run(self) -> Dataset:
        """
        Executes the processing pipeline by applying all tasks to the source dataset,
        updating the target dataset, and persisting both the dataset and metadata.

        The method performs the following steps:
        1. Executes each task sequentially, applying them to the source dataset.
        2. Updates the target dataset with the final processed data.
        3. Persists the target dataset in the repository.
        4. Persists the passport associated with the target dataset in the flow state.

        Args:
            None

        Returns:
            Dataset: The processed target dataset, updated and persisted.

        Raises:
            RuntimeError: If any task execution fails, an error is logged and the exception is raised.
        """
        # Step 1: Execute all tasks sequentially
        data = self._source.dataframe
        for task in self._tasks:
            try:
                data = task.run(data)
            except Exception as e:
                msg = f"Error in task {task.__class__.__name__}: {e}"
                self._logger.error(msg)
                raise RuntimeError(msg)

        # Step 2: Update the target dataset with the final processed data
        self._target = self._set_target_dataframe(dataset=self._target, dataframe=data)

        # Step 3: Persist the stage in the repository
        self._repo.add(asset=self._target)

        # Step 4: Persist the passport in the flowstate
        self._state.create(passport=self._target.passport)

        return self._target

    def _set_target_dataframe(
        self,
        dataset: Dataset,
        dataframe: Union[pd.DataFrame, pd.core.frame.DataFrame, DataFrame],
    ) -> Dataset:
        """A cheeky back door rather than exposing a setter on the dataset."""
        setattr(dataset, "_dataframe", dataframe)
        return dataset
