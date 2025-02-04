#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/tqa/builder.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:14:25 am                                                #
# Modified   : Tuesday February 4th 2025 03:19:51 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Builder Module"""
from __future__ import annotations

from typing import Optional, Type

from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.dataprep.tqa.stage import TQAStage
from genailab.flow.dataprep.tqa.task import (
    DATASET_SCHEMA,
    TQAnalystDask,
    TQAnalystPandas,
    TQATask,
)
from genailab.infra.config.app import AppConfigReader


# ------------------------------------------------------------------------------------------------ #
class TQAStageBuilder(StageBuilder):
    """Builds a TQA (Text Quality Analysis) stage for Pandas or Dask-based processing.

    This class constructs a TQAStage by configuring and adding tasks for text quality
    analysis using either Pandas or Dask. It allows setting processing configurations
    such as normalization, batching, and parallelization.

    Args:
        appconfig_reader_cls (Type[AppConfigReader], optional): The configuration reader class
            to use for retrieving app settings. Defaults to AppConfigReader.

    Attributes:
        _appconfig_reader (AppConfigReader): Reads configurations for the TQA stage.
        _dftype (Optional[DFType]): Specifies whether Pandas or Dask will be used.
        _source_config (Optional[DatasetConfig]): The source dataset configuration.
        _target_config (Optional[DatasetConfig]): The target dataset configuration.
        _tqa_task (Optional[Dict]): The TQA task configuration.
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.TQA

    def __init__(self, appconfig_reader_cls: Type[AppConfigReader] = AppConfigReader) -> None:
        super().__init__()
        self._appconfig_reader = appconfig_reader_cls()
        self._dftype = None
        self.reset()

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase definition for TQA."""
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage definition for TQA."""
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the type of DataFrame (Pandas or Dask) set for the stage."""
        return self._dftype

    def reset(self) -> None:
        """Resets the builder to its initial state."""
        super().reset()
        self._source_config = None
        self._target_config = None
        self._tqa_task = None
        self._dftype = None


    def with_pandas(self, normalized: bool = True, batched: bool = True, **kwargs) -> TQAStageBuilder:
        """Configures the stage to use a Pandas-based TQAnalyst.

        This method sets the DataFrame type to Pandas and initializes a TQAnalystPandas instance
        using configurations from the application settings.

        Args:
            normalized (bool, optional): Whether to normalize text quality scores. Defaults to True.
            batched (bool, optional): Whether to process data in batches. Defaults to True.
            **kwargs: Additional keyword arguments for analyst configuration.

        Returns:
            TQAStageBuilder: The updated builder instance.
        """
        self._dftype = DFType.PANDAS
        self._tqa_task = self._task_configs['tqa']
        self._tqa_task['params']['normalized'] = normalized
        self._tqa_task['params']['batched'] = batched

        config = self._appconfig_reader.get_config(section='dask', namespace=True)

        analyst = TQAnalystPandas(
            coefficients=self._tqa_task["params"]["coefficients"],
            normalized=self._tqa_task["params"]["normalized"],
            npartitions=config.npartitions,
            batched=batched,
            **kwargs
        )

        task = TQATask(analyst=analyst)
        self._tasks.append(task)
        return self

    def with_dask(self, normalized: bool = True, batched: bool = True, **kwargs) -> TQAStageBuilder:
        """Configures the stage to use a Dask-based TQAnalyst.

        This method sets the DataFrame type to Dask and initializes a TQAnalystDask instance
        using configurations from the application settings.

        Args:
            normalized (bool, optional): Whether to normalize text quality scores. Defaults to True.
            batched (bool, optional): Whether to process data in batches. Defaults to True.
            **kwargs: Additional keyword arguments for analyst configuration.

        Returns:
            TQAStageBuilder: The updated builder instance.
        """
        self._dftype = DFType.DASK
        config = self._appconfig_reader.get_config(section='dask', namespace=True)
        self._tqa_task = self._task_configs['tqa']
        self._tqa_task['params']['normalized'] = normalized
        self._tqa_task['params']['batched'] = batched

        analyst = TQAnalystDask(
            schema=DATASET_SCHEMA,
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

        task = TQATask(analyst=analyst)
        self._tasks.append(task)
        return self

    def build(self,
              source_config: Optional[DatasetConfig] = None,
              target_config: Optional[DatasetConfig] = None,
              strict: bool = True) -> TQAStage:
        """Builds the TQAStage with the configured source, target, and tasks.

        This method finalizes the configuration and returns a new `TQAStage` instance.

        Args:
            source_config (Optional[DatasetConfig], optional): The source dataset configuration.
                Defaults to None.
            target_config (Optional[DatasetConfig], optional): The target dataset configuration.
                Defaults to None.
            strict (bool, optional): Whether to enforce strict validation. Defaults to True.

        Returns:
            TQAStage: The constructed TQA stage.

        Raises:
            ValueError: If required configurations are missing.
        """
        self._validate(strict=strict)

        stage = TQAStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=self._tasks,
            repo=self._repo,
            dataset_builder=self._dataset_builder,
        )
        self.reset()
        return stage

    def _validate(self, strict: bool = True) -> None:
        """Validates the current builder configuration.

        Ensures that at least one TQA task has been configured before building the stage.

        Args:
            strict (bool, optional): Whether to enforce strict validation. Defaults to True.

        Raises:
            ValueError: If validation fails.
        """
        errors = []
        if self._tqa_task is None:
            errors.append("No TQA Task was set.")

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
