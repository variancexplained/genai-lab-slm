#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/sentiment/stage.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 07:01:12 pm                                              #
# Modified   : Tuesday November 19th 2024 08:06:36 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Sentiment Analysis Stage Module"""
from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_prep.base.stage import DataPrepStage
from discover.infra.config.app import AppConfigReader


# ------------------------------------------------------------------------------------------------ #
class SentimentAnalysisStage(DataPrepStage):
    """
    Stage for sentiment analysis in the data preparation pipeline.

    This class applies sentiment analysis to the input data as part of the
    data preparation process. It inherits from `DataPrepStage` and is responsible
    for configuring and running tasks related to sentiment analysis, ultimately
    saving the processed data to the destination.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
        return_dataset (bool): Whether to return resultant dataset or its asset_id
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        return_dataset: bool = False,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
            return_dataset=return_dataset,
        )

        env = AppConfigReader().get_environment()
        self._cache = f"{self._cache}_{env}.csv"

    def _run(self, source_asset_id: str, destination_asset_id: str) -> None:
        """Performs the core logic of the stage, executing tasks in sequence.

        Args:
            source_asset_id (str): The asset identifier for the source dataset.
            destination_asset_id (str): The asset identifier for the destination dataset.

        Raises:
            RuntimeError: Running on local machine is not supported.
        """
        raise RuntimeError("Sentiment Analysis is not supported on local machine.")
