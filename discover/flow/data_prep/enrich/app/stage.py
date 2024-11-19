#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/enrich/app/stage.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 19th 2024 03:08:02 am                                              #
# Modified   : Tuesday November 19th 2024 03:16:09 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""App Enrichment Stage Module"""
from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_prep.base.stage import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class AppEnrichmentStage(DataPrepStage):
    """Stage for enriching app data with various metrics and deviations.

    This class adds enrichment features to app data, including app rating,
    perplexity, review length, review age, voting counts, and deviations
    of these metrics relative to category-level aggregates. This enrichment
    provides a comprehensive view of each app's performance and characteristics
    compared to its category, facilitating in-depth analysis and comparisons.

    Args:
        phase (PhaseDef): The phase definition indicating where this stage
            fits within the overall data processing workflow.
        stage (StageDef): The stage definition providing details on the
            configuration and behavior of this stage.
        source_config (dict): Configuration for the data source, including
            connection parameters and data retrieval settings.
        destination_config (dict): Configuration for the data destination,
            specifying where to store the enriched data.
        force (bool): Whether to force the execution of this stage, even if
            it has been previously completed. Defaults to False.
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
