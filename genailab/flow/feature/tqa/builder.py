#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/feature/tqa/builder.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:14:25 am                                                #
# Modified   : Friday January 31st 2025 03:52:32 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Builder Module"""
from __future__ import annotations

from copy import deepcopy
from typing import Optional, Type

from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.feature.tqa.stage import TQAStage
from genailab.flow.feature.tqa.task import (
    DATASET_SCHEMA,
    TQAnalystDask,
    TQAnalystPandas,
    TQATask,
)
from genailab.infra.config.app import AppConfigReader


# ------------------------------------------------------------------------------------------------ #
class TQAStageBuilder(StageBuilder):

    __PHASE = PhaseDef.FEATURE
    __STAGE = StageDef.TQA


    def __init__(self, appconfig_reader_cls: Type[AppConfigReader] = AppConfigReader) -> None:
        super().__init__()
        self._appconfig_reader = appconfig_reader_cls()
        self._dftype = None
        self.reset()

    @property
    def phase(self) -> PhaseDef:
        """
        The phase of the pipeline associated with the preprocess stage.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """
        The stage of the pipeline associated with the preprocess stage.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """
        Defines the dataframe type of the pipeline.

        Returns:
            DFType: The dataframe type used in the pipeline.
        """
        return self._dftype

    def reset(self) -> None:
        """Resets the builder."""
        super().reset()
        self._source_config = None
        self._target_config = None
        self._tqa_task = None
        self._dftype = None


    def with_pandas(self, normalized: bool = True, batched: bool = True, **kwargs) -> TQAStageBuilder:
        """Constructs a TQAnalyst for Pandas"""
        # Set the data frame type
        self._dftype = DFType.PANDAS
        # Obtain the task configuration
        self._tqa_task = self._task_configs['tqa']
        # Update the config
        self._tqa_task['params']['normalized'] = normalized
        self._tqa_task['params']['batched'] = batched
        # Obtain the analyst config
        config = self._appconfig_reader.get_config(section='dask', namespace=True)
        # Instantiate the analyst
        analyst = TQAnalystPandas(coefficients=self._tqa_task["params"]["coefficients"],
                                  normalized=self._tqa_task["params"]["normalized"],
                                  npartitions=config.npartitions,
                                  batched=batched, **kwargs)
        # Construct the TQATask
        task = TQATask(analyst=analyst)
        # Append the task to the task list.
        self._tasks.append(task)
        return self

    def with_dask(self, normalized: bool = True, batched: bool = True, **kwargs) -> TQAStageBuilder:
        """Constructs a TQAnalyst for Dask"""
        # Set the data frame type
        self._dftype = DFType.DASK
        # Obtain the analyst config
        config = self._appconfig_reader.get_config(section='dask', namespace=True)
        # Obtain the task configuration
        self._tqa_task = self._task_configs['tqa']
        # Update the config
        self._tqa_task['params']['normalized'] = normalized
        self._tqa_task['params']['batched'] = batched
        # Construct the analyst
        analyst = TQAnalystDask(schema=DATASET_SCHEMA,
                                coefficients=self._tqa_task["params"]["coefficients"],
                                npartitions=config.npartitions,
                                normalized=normalized,
                                batched=batched,
                                nworkers=config.nworkers,
                                memory_limit=config.memory_limit,
                                threads_per_worker=config.threads_per_worker,
                                processes=config.processes,
                                kwargs=kwargs
                               )
        # Construct the task
        task = TQATask(analyst=analyst)
        # Append the task to the task list.
        self._tasks.append(task)
        return self
    # -------------------------------------------------------------------------------------------- #

    def build(self,
        source_config: Optional[DatasetConfig] = None,
        target_config: Optional[DatasetConfig] = None,
        strict: bool = True,
        ) -> TQAStage:
        """
        Builds the Text Quality Analysis - Syntactic Stage by validating configurations,
        assembling the tasks and returning the configured stage.

        Args:
            source_config (Optional[DatasetConfig]): An optional configuration object for
                the source dataset. If not provided, the method falls back to the source
                configuration defined in the stage YAML config.
            target_config (Optional[DatasetConfig]): An optional configuration object for
                the target dataset. If not provided, the method falls back to the target
                configuration defined in the stage YAML config.
            strict (bool): Whether strict, more thorough validation during build process.

        Returns:
            TQASyntacticStage: The builder instance with the constructed stage.
        """

        self._validate(strict=strict)

        stage = TQAStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=deepcopy(self._tasks),
            repo=self._repo,
            dataset_builder=self._dataset_builder,
        )
        self.reset()
        return stage

    def _validate(self, strict: bool = True) -> None:
        """
        Validates the configurations and settings for the TQASyntactic Stage.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        # super()._validate()
        errors = []
        if self._tqa_task is None:
            errors.append("No TQA Task was set.")

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
