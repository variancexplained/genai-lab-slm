#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/perplexity/stage.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 07:01:12 pm                                              #
# Modified   : Tuesday November 19th 2024 04:41:19 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Perplexity Stage Module"""

from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_prep.base.stage import DataPrepStage
from discover.infra.config.flow import FlowConfigReader


# ------------------------------------------------------------------------------------------------ #
class PerplexityAnalysisStage(DataPrepStage):
    """Stage for performing perplexity analysis on text data.

    This class is responsible for orchestrating the perplexity analysis
    as part of a data preparation pipeline. It manages the configuration
    and execution of tasks that compute perplexity scores for text data.

    Args:
        phase (PhaseDef): The phase definition indicating where this stage fits
            within the overall data processing workflow.
        stage (StageDef): The stage definition providing details on the configuration
            and behavior of this stage.
        source_config (dict): Configuration for the data source, including connection
            parameters and data retrieval settings.
        destination_config (dict): Configuration for the data destination, specifying
            where to store the processed data.
        force (bool): Whether to force the execution of this stage, even if it has
            been previously completed. Defaults to False.
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
        )

        env = FlowConfigReader().get_environment()
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
