#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data/data_prep/enrich.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 22nd 2024 12:15:07 am                                               #
# Modified   : Thursday December 19th 2024 01:40:48 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Enrichment Stage Module"""
from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.data_prep.base import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class DataEnrichmentStage(DataPrepStage):
    """
    A stage in the data preparation pipeline that performs dataset enrichment by adding
    new features or transformations to enhance downstream analysis.

    Args:
        phase (PhaseDef): The phase of the pipeline to which this stage belongs.
        stage (StageDef): The specific stage identifier within the phase.
        source_config (dict): Configuration for the source dataset, including input paths and schema details.
        destination_config (dict): Configuration for the destination dataset, specifying where enriched data will be saved.
        force (bool, optional): If True, forces the stage to re-run even if outputs already exist. Defaults to False.
        return_dataset (bool, optional): If True, returns the enriched dataset after processing. Defaults to False.
        **kwargs: Additional arguments to support custom functionality or configurations.
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: StageDef,
        source_config: dict,
        destination_config: dict,
        force: bool = False,
        return_dataset: bool = False,
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
