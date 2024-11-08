#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/metadata/stage.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 10:15:10 pm                                              #
# Modified   : Thursday November 7th 2024 11:54:13 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from typing import List

from pyspark.sql import DataFrame

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, EnrichmentStageDef, PhaseDef
from discover.flow.base.task import Task
from discover.flow.enrich.stage import EnrichmentStage

# ------------------------------------------------------------------------------------------------ #


class MetadataStage(EnrichmentStage):
    """
    A class that represents a stage in the data processing pipeline dedicated to enriching
    metadata features, such as review length and review age, in the dataset.

    This stage handles loading the source data, applying specified tasks, and saving the
    enriched data to the destination. It also generates a unique asset ID for the destination
    using configuration details.

    Attributes:
        source_config (dict): Configuration details for loading the source data asset.
        destination_config (dict): Configuration details for saving the enriched data asset.
        tasks (List[Task]): A list of tasks to be executed for metadata enrichment.
        force (bool): Whether to force the execution of the stage, overriding existing assets.
        **kwargs: Additional keyword arguments for customization and flexibility.

    Methods:
        _load_source_data() -> DataFrame:
            Loads the source dataset from the repository using the source asset ID.
            If a pandas index column is present in the dataset, it is renamed to "pandas_index".
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
            stage=EnrichmentStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def _load_source_data(self) -> DataFrame:
        """Loads the source dataset from the repository using the source asset ID.

        Returns:
            DataFrame: The content of the source dataset as a PySpark DataFrame. If the dataset
            contains a pandas index column named "__index_level_0__", it is renamed to "pandas_index".
        """
        source_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._source_config.asset_type,
            phase=PhaseDef.from_value(value=self._source_config.phase),
            stage=DataPrepStageDef.from_value(value=self._source_config.stage),
            name=self._source_config.name,
        )
        dataset = self._repo.get(asset_id=source_asset_id, distributed=True, nlp=True)
        # Rename the pandas index column if it exists
        if "__index_level_0__" in dataset.content.columns:
            dataset.content = dataset.content.withColumnRenamed(
                "__index_level_0__", "pandas_index"
            )
        return dataset.content
