#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/sa/builder.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:14:25 am                                                #
# Modified   : Tuesday February 4th 2025 02:37:31 am                                               #
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
from genailab.flow.dataprep.sa.stage import SentimentAnalysisStage
from genailab.infra.config.app import AppConfigReader


# ------------------------------------------------------------------------------------------------ #
class SentimentAnalysisStageBuilder(StageBuilder):
    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.SENTIMENT
    __DFTYPE = DFType.PANDAS

    def __init__(self, appconfig_reader_cls: Type[AppConfigReader] = AppConfigReader) -> None:
        super().__init__()
        self._appconfig_reader = appconfig_reader_cls()
        self.reset()

    @property
    def phase(self) -> PhaseDef:
        """Returns the phase definition for SentimentAnalysis."""
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """Returns the stage definition for SentimentAnalysis."""
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """Returns the type of DataFrame (Pandas or Dask) set for the stage."""
        return self.__DFTYPE

    def reset(self) -> None:
        """Resets the builder to its initial state."""
        super().reset()
        self._source_config = None
        self._target_config = None
        self._task_configs = self._get_config(
            phase=self.phase, stage=self.stage, config="tasks"
        )
        self._tasks = []


    def build(self,
              source_config: Optional[DatasetConfig] = None,
              target_config: Optional[DatasetConfig] = None,
              strict: bool = True) -> SentimentAnalysisStage:
        """Builds the SentimentAnalysisStage with the configured source, target, and tasks.

        This method finalizes the configuration and returns a new `SentimentAnalysisStage` instance.

        Args:
            source_config (Optional[DatasetConfig], optional): The source dataset configuration.
                Defaults to None.
            target_config (Optional[DatasetConfig], optional): The target dataset configuration.
                Defaults to None.
            strict (bool, optional): Whether to enforce strict validation. Defaults to True.

        Returns:
            SentimentAnalysisStage: The constructed SentimentAnalysis stage.

        Raises:
            ValueError: If required configurations are missing.
        """
        self._validate(strict=strict)

        # Construct the merge task to merge sentiment into the data
        mergetask = self._task_configs["sentiment"]
        self._tasks.append(self._task_builder.build(mergetask))

        stage = SentimentAnalysisStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=deepcopy(self._tasks),
            repo=self._repo,
            dataset_builder=self._dataset_builder,
        )
        self.reset()
        return stage

    def _validate(self, strict: bool = True) -> None:
        """Validates the current builder configuration.

        Ensures that at least one SentimentAnalysis task has been configured before building the stage.

        Args:
            strict (bool, optional): Whether to enforce strict validation. Defaults to True.

        Raises:
            ValueError: If validation fails.
        """
        pass