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
# Modified   : Monday November 11th 2024 03:35:50 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import List

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.data_prep.stage import DataPrepStage


# ------------------------------------------------------------------------------------------------ #
class SentimentClassificationStage(DataPrepStage):
    """
    A class that represents a stage in the data processing pipeline dedicated to
    classifying sentiment in textual data.

    This stage is responsible for loading the source data, applying sentiment classification
    tasks, and saving the processed data to the specified destination. It also generates a
    unique asset ID for the destination using the provided configuration details.

    Attributes:
        source_config (dict): Configuration details for loading the source data asset.
        destination_config (dict): Configuration details for saving the classified data asset.
        tasks (List[Task]): A list of tasks to be executed for sentiment classification.
        force (bool): Whether to force the execution of the stage, overriding existing assets.
        **kwargs: Additional keyword arguments for customization and flexibility.

    Methods:
        _load_source_data() -> pd.DataFrame:
            Loads the source dataset from the repository using the source asset ID.
    """

    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        **kwargs,
    ) -> None:
        """
        Initializes the MetadataEnrichmentStage with source and destination configurations,
        a list of tasks for metadata enrichment, and an optional force flag.

        Args:
            source_config (dict): Configuration details for the source data asset.
            destination_config (dict): Configuration details for the destination data asset.
            tasks (List[Task]): A list of tasks to execute during the metadata enrichment stage.
            force (bool): If True, forces the execution of the stage, even if the destination asset exists.
            **kwargs: Additional keyword arguments for flexibility and customization.
        """
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._destination_config.asset_type,
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=DataPrepStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
