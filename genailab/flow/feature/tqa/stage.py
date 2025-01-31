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
# Modified   : Friday January 31st 2025 04:44:17 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""TQA Stage Module"""
import inspect
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

    __PHASE = PhaseDef.FEATURE
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
        """Returns the phase of the pipeline."""
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage of the pipeline."""
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the data frame type used in this stage."""
        return self.__DFTYPE

    def _run(self) -> Dataset:
        """Executes the stage tasks and saves the resulting dataset.

        Returns:
            Dataset: The processed dataset.
        """
        self._logger.debug(f"Inside {self.__class__.__name__}: {inspect.currentframe().f_code.co_name}")
        # Remove existing target dataset if it exists.
        self._remove_dataset(config=self._target_config)

        # Obtain the source dataset from the repo.
        source = self._get_dataset(config=self._source_config)
        dataframe = source.dataframe

        # Clean text
        dataframe['content'] = dataframe['content'].apply(self._clean_text)

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


    def _clean_text(self, text):
        """
        Cleans the input text by:
        - Lowercasing
        - Removing special characters
        - Removing extra spaces
        - Removing numbers (optional)
        """
        # Lowercasing the text
        text = text.lower()

        # Remove special characters and numbers (optional, modify as needed)
        text = re.sub(r'[^a-zA-Z\s]', '', text)

        # Remove extra whitespace
        text = ' '.join(text.split())

        return text
