#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/sa/stage.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:26:44 am                                                #
# Modified   : Saturday February 8th 2025 10:43:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""TQA Stage Module"""
from typing import List

from genailab.asset.dataset.builder import DatasetBuilder
from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.stage import Stage
from genailab.flow.base.task import Task
from genailab.infra.persist.repo.dataset import DatasetRepo


# ------------------------------------------------------------------------------------------------ #
class SentimentAnalysisStage(Stage):

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.SENTIMENT
    __DFTYPE = DFType.PANDAS

    def __init__(
        self,
        source_config: DatasetConfig,
        target_config: DatasetConfig,
        tasks: List[Task],
        repo: DatasetRepo,
        dataset_builder: DatasetBuilder,
    ) -> None:
        super().__init__(
            source_config=source_config,
            target_config=target_config,
            tasks=tasks,
            repo=repo,
            dataset_builder=dataset_builder,
        )

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


