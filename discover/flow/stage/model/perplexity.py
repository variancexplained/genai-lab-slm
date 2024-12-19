#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/model/perplexity.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 10:51:29 pm                                             #
# Modified   : Thursday December 19th 2024 01:40:50 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""ing stage class module."""
from __future__ import annotations

from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.base import Stage


# ------------------------------------------------------------------------------------------------ #
#                            PERPLEXITY ANALYSIS MODEL STAGE                                       #
# ------------------------------------------------------------------------------------------------ #
class PerplexityAnalysisStage(Stage):
    """
    Stage for performing perplexity analysis in a data pipeline.

    This class calculates perplexity metrics to evaluate the fluency or predictability
    of textual data. It inherits from the base `Stage` class and allows configuration
    of source and destination settings.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source, including details
            such as file paths or database connections.
        destination_config (dict): Configuration for the data destination, including
            details on where to save processed results.
        force (bool, optional): Whether to force execution, even if the destination
            dataset already exists. Defaults to False.
        return_dataset (bool, optional): Whether to return the resultant dataset
            instead of only the asset ID. Defaults to True.
        **kwargs: Additional keyword arguments for custom configurations.
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        return_dataset: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
            return_dataset=return_dataset,
            **kwargs,
        )
