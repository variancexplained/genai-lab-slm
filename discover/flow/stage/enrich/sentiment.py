#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/enrich/sentiment.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 25th 2024 06:17:19 pm                                            #
# Modified   : Wednesday December 25th 2024 06:57:25 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Sentiment Classification Stage Module"""
from discover.core.flow import DataEnrichmentStageEnum, PhaseEnum
from discover.flow.stage.enrich.base import DataEnrichmentStage


# ------------------------------------------------------------------------------------------------ #
class SentimentClassificationStage(DataEnrichmentStage):
    """
    The sentiment classification stage in the data enrichment phase.

    Args:
        phase (PhaseEnum): The phase of the pipeline to which this stage belongs.
        stage (DataEnrichmentStageEnum): The specific stage identifier within the phase.
        source_config (dict): Configuration for the source dataset, including input paths and schema details.
        destination_config (dict): Configuration for the destination dataset, specifying where enriched data will be saved.
        force (bool, optional): If True, forces the stage to re-run even if outputs already exist. Defaults to False.
        **kwargs: Additional arguments to support custom functionality or configurations.
    """

    def __init__(
        self,
        phase: PhaseEnum,
        stage: DataEnrichmentStageEnum,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            phase=phase,
            stage=stage,
            source_config=source_config,
            destination_config=destination_config,
            force=force,
            **kwargs,
        )
