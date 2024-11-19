#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/enrich/category/stage.py                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 19th 2024 03:08:02 am                                              #
# Modified   : Tuesday November 19th 2024 03:14:31 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Category Enrichment Stage Module"""
from discover.core.flow import PhaseDef, StageDef
from discover.flow.data_prep.base.stage import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class CategoryEnrichmentStage(DataPrepStage):
    """Stage for aggregating metrics at the category level.

    This class aggregates various metrics at the category level, including
    the number of apps, average ratings, rating statistics, review length
    statistics, sentiment distributions, and perplexity scores. These
    aggregated metrics provide insights at a higher level, useful for
    comparative analysis and category-wide assessments.

    Args:
        phase (PhaseDef): The phase definition indicating where this stage
            fits within the overall data processing workflow.
        stage (StageDef): The stage definition providing details on the
            configuration and behavior of this stage.
        source_config (dict): Configuration for the data source, including
            connection parameters and data retrieval settings.
        destination_config (dict): Configuration for the data destination,
            specifying where to store the aggregated metrics.
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
