#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data/data_prep/quality.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 22nd 2024 12:15:07 am                                               #
# Modified   : Friday December 27th 2024 05:21:00 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Stage Module"""
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.stage.base import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class DataQualityStage(DataPrepStage):
    """
    Stage for cleaning data in the data preparation pipeline.

    This class inherits from `DataPrepStageDef` and focuses on data cleaning tasks
    to ensure data quality and consistency before further processing. It sets up
    the configuration and execution logic needed for data cleaning.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (DataPrepStageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
        return_dataset (bool): Whether to return resultant dataset or its asset_id
    """

    def __init__(
        self,
        phase: PhaseDef,
        stage: DataPrepStageDef,
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
