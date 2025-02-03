#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/feature/tqa/stage.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:26:44 am                                                #
# Modified   : Monday February 3rd 2025 05:22:26 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""TQA Stage Module"""
import re
from typing import List

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.asset.dataset.dataset import Dataset
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.stage import Stage
from genailab.flow.base.task import Task
from genailab.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class TQAStage(Stage):
    """Represents the Text Quality Analysis (TQA) stage in a data processing pipeline.

    This stage applies text cleaning and various tasks related to text quality analysis.
    It processes a dataset by executing configured tasks and stores the resulting dataset
    in the repository.

    Args:
        source_config (DatasetConfig): Configuration for the source dataset.
        target_config (DatasetConfig): Configuration for the target dataset.
        tasks (List[Task]): List of tasks to execute in this stage.
        repo (DatasetRepo): Repository managing dataset storage and retrieval.
        dataset_builder (DatasetBuilder): Builder for dataset creation.
        column (str, optional): The column containing text data to process. Defaults to "content".

    Attributes:
        _column (str): Column name containing text data.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.TQA
    __DFTYPE = DFType.PANDAS

    def __init__(
        self,
        source_config: DatasetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
        column: str = "content",
    ) -> None:
        super().__init__(
            source_config=source_config,
            target_config=target_config,
            tasks=tasks,
            repo=repo,
            dataset_builder=dataset_builder,
        )
        self._column = column

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase of the pipeline.

        Returns:
            PhaseDef: The phase this stage belongs to.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage of the pipeline.

        Returns:
            StageDef: The stage this component represents.
        """
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the data frame type used in this stage.

        Returns:
            DFType: The type of DataFrame (e.g., Pandas, Dask).
        """
        return self.__DFTYPE

    def _run(self) -> Dataset:
        """Executes the stage tasks and saves the resulting dataset.

        This method:
        - Loads the source dataset.
        - Applies text cleaning.
        - Runs each task sequentially.
        - Creates and stores the processed dataset.

        Returns:
            Dataset: The processed dataset.

        Raises:
            RuntimeError: If a task encounters an error.
        """
        # Remove existing target dataset if it exists.
        self._remove_dataset(config=self._target_config)

        # Obtain the source dataset from the repo.
        source = self._get_dataset(config=self._source_config)
        dataframe = source.dataframe

        # Clean text
        dataframe[self._column] = dataframe[self._column].apply(self._clean_text)

        for task in self._tasks:
            try:
                dataframe = task.run(dataframe)
            except Exception as e:
                self._logger.error(f"Error in task {task.__class__.__name__}: {e}")
                raise RuntimeError(f"Error in task {task.__class__.__name__}: {e}")

        target = self._create_dataset(
            source=source.passport, config=self._target_config, dataframe=dataframe
        )
        target = self._repo.add(dataset=target, entity=self.__class__.__name__)

        source.consume(entity=self.__class__.__name__)
        self._repo.update(dataset=source)

        return target

    def _clean_text(self, text: str) -> str:
        """Cleans the input text by applying:

        - Lowercasing
        - Removing special characters
        - Removing extra spaces
        - Removing numbers (optional)

        Args:
            text (str): The input text string.

        Returns:
            str: The cleaned text.
        """
        # Lowercase the text
        text = text.lower()

        # Remove special characters and numbers (optional, modify as needed)
        text = re.sub(r'[^a-zA-Z\s]', '', text)

        # Remove extra whitespace
        text = ' '.join(text.split())

        return text
