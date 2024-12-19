#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/analysis/perplexity.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 07:01:12 pm                                              #
# Modified   : Thursday December 19th 2024 01:40:50 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Perplexity Stage Module"""

from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.data_prep.base import DataPrepStage


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
