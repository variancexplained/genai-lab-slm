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
# Modified   : Thursday January 30th 2025 10:53:46 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Builder Module"""
from __future__ import annotations

from typing import Optional

from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.feature.tqa.stage import TQAStage


# ------------------------------------------------------------------------------------------------ #
class TQAStageBuilder(StageBuilder):

    __PHASE = PhaseDef.FEATURE
    __STAGE = StageDef.TQA
    __DFTYPE = DFType.PANDAS

    def __init__(self) -> None:
        super().__init__()
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
        return self.__DFTYPE

    def reset(self) -> None:
        super().reset()
        self._source_config = None
        self._target_config = None
        self._tqa_task = None
        self._task_configs = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )

    # -------------------------------------------------------------------------------------------- #
    #                                       TASK                                                   #
    # -------------------------------------------------------------------------------------------- #
    def tqa(self, normalized: bool = True) -> TQAStageBuilder:
        self._tqa_task = self._task_configs['tqa']
        self._tqa_task['params']['normalized'] = normalized
        self._tasks.append(self._task_builder.build(self._tqa_task))
        return self
    # -------------------------------------------------------------------------------------------- #
    #                                      BUILD                                                   #
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
            tasks=self._tasks,
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
        super()._validate()
        errors = []
        if self._tqa_task is None:
            errors.append("A Spark session is required for the TQASyntactic Stage.")

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
