#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/flow/feature/tqa/syntactic/builder.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:14:25 am                                                #
# Modified   : Saturday January 25th 2025 04:41:08 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Builder Module"""
from __future__ import annotations

from genailabslm.core.flow import PhaseDef, StageDef
from genailabslm.flow.base.builder import StageBuilder
from genailabslm.flow.feature.tqa.syntactic.stage import TQASyntacticStage


# ------------------------------------------------------------------------------------------------ #
class TQASyntacticStageBuilder(StageBuilder):

    __PHASE = PhaseDef.FEATURE
    __STAGE = StageDef.TQA_SYNTACTIC

    def __init__(self) -> None:
        super().__init__()
        self.reset()

    def reset(self) -> None:
        super().reset()
        self._source_config = None
        self._target_config = None

        self._noun_phrase_count = None
        self._adjective_noun_pair_count = None
        self._aspect_related_verb_count = None
        self._adverb_phrase_count = None
        self._noun_count = None
        self._verb_count = None
        self._adverb_count = None
        self._comparative_adjective_count = None
        self._superlative_adjective_count = None

        self._task_configs = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )

    # -------------------------------------------------------------------------------------------- #
    #                                 SOURCE AND TARGET DATASETS                                   #
    # -------------------------------------------------------------------------------------------- #
    def source(self, source_config: DatasetConfig) -> TQASyntacticStageBuilder:
        self._source_config = source_config
        return self

    # -------------------------------------------------------------------------------------------- #
    def target(self, target_config: DatasetConfig) -> TQASyntacticStageBuilder:
        self._target_config = target_config
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                           TASKS                                              #
    # -------------------------------------------------------------------------------------------- #
    def noun_phrase_count(self) -> TQASyntacticStageBuilder:
        self._noun_phrase_count = self._task_configs["noun_phrase_count"]
        self._tasks.append(self._task_builder.build(self._noun_phrase_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def adjective_noun_pair_count(self) -> TQASyntacticStageBuilder:
        self._adjective_noun_pair_count = self._task_configs[
            "adjective_noun_pair_count"
        ]
        self._tasks.append(self._task_builder.build(self._adjective_noun_pair_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def aspect_verb_pair_count(self) -> TQASyntacticStageBuilder:
        self._aspect_verb_pair_count = self._task_configs["aspect_verb_pair_count"]
        self._tasks.append(self._task_builder.build(self._aspect_verb_pair_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def adverb_phrase_count(self) -> TQASyntacticStageBuilder:
        self._adverb_phrase_count = self._task_configs["adverb_phrase_count"]
        self._tasks.append(self._task_builder.build(self._adverb_phrase_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def noun_count(self) -> TQASyntacticStageBuilder:
        self._noun_count = self._task_configs["noun_count"]
        self._tasks.append(self._task_builder.build(self._noun_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def verb_count(self) -> TQASyntacticStageBuilder:
        self._verb_count = self._task_configs["verb_count"]
        self._tasks.append(self._task_builder.build(self._verb_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def adverb_count(self) -> TQASyntacticStageBuilder:
        self._adverb_count = self._task_configs["adverb_count"]
        self._tasks.append(self._task_builder.build(self._adverb_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    def adjective_count(self) -> TQASyntacticStageBuilder:
        self._adjective_count = self._task_configs["adjective_count"]
        self._tasks.append(self._task_builder.build(self._adjective_count))
        return self

    # -------------------------------------------------------------------------------------------- #
    #                                      BUILD                                                   #
    # -------------------------------------------------------------------------------------------- #
    def build(self) -> TQASyntacticStageBuilder:
        self._validate()
        self._stage = TQASyntacticStage(
            source_config=self._source_config,
            target_config=self._target_config,
            tasks=self._tasks,
            state=self._state,
            repo=self._repo,
            dataset_builder=self._dataset_builder,
            spark=self._spark,
        )
        return self

    def _validate(self) -> None:
        """
        Validates the configurations and settings for the TQASyntactic Stage.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if self._spark is None:
            errors.append("A Spark session is required for the TQASyntactic Stage.")

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
