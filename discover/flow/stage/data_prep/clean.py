#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/stage/data_prep/clean.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday October 19th 2024 12:57:59 pm                                              #
# Modified   : Sunday December 15th 2024 02:54:09 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Cleaning Stage Module"""
import pandas as pd

from discover.assets.dataset import Dataset
from discover.core.flow import PhaseDef, StageDef
from discover.flow.stage.data_prep.base import DataPrepStage
from discover.infra.service.logging.stage import stage_logger
from discover.infra.utils.data.compare import compare_dataframes
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
COLUMNS = [
    "id",
    "app_id",
    "app_name",
    "category_id",
    "author",
    "rating",
    "content",
    "vote_sum",
    "vote_count",
    "date",
    "review_length",
    "pa_perplexity",
    "sa_sentiment",
]


# ------------------------------------------------------------------------------------------------ #
class DataCleaningStage(DataPrepStage):
    """
    Stage for cleaning data in the data preparation pipeline.

    This class inherits from `DataPrepStage` and focuses on data cleaning tasks
    to ensure data quality and consistency before further processing. It sets up
    the configuration and execution logic needed for data cleaning.

    Args:
        phase (PhaseDef): The phase of the data pipeline.
        stage (StageDef): The specific stage within the data pipeline.
        source_config (dict): Configuration for the data source.
        destination_config (dict): Configuration for the data destination.
        force (bool, optional): Whether to force execution, even if the output already
            exists. Defaults to False.
        return_dataset (bool): Whether to return resultant dataset or its asset_id
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

    @stage_logger
    def run(self) -> Dataset:
        """Executes the stage and returns the asset ID.

        This method determines the execution path based on whether the endpoint
        exists and whether the `force` flag is set. It runs tasks to prepare
        the data or updates the endpoint if changes are detected in the source data.

        Returns:
            str: The asset ID of the prepared dataset.
        """
        result = super()._core_stage_run()
        # Reload original and clean datasets
        orig_dataset = self._load_dataset(
            asset_id=self.source_asset_id, dataframe_type=self.source_dataframe_type
        )
        clean_dataset = self._load_dataset(
            asset_id=self.destination_asset_id,
            dataframe_type=self.destination_dataframe_type,
        )

        # Summarize the data cleaning operation.
        summary = self._summarize(
            orig=orig_dataset.content, clean=clean_dataset.content
        )
        Printer().print_subheader("Data Cleaning Analysis")
        print(summary)
        return result

    def _summarize(self, orig: pd.DataFrame, clean: pd.DataFrame) -> None:
        comparison = compare_dataframes(
            self_df=orig,
            other_df=clean,
            cols=COLUMNS,
        )
        comparison_dict = {
            "Rows": [
                orig.shape[0],
                clean.shape[0],
                round((orig.shape[0] - clean.shape[0]) / orig.shape[0] * 100, 2),
            ],
            "Rows Modified": [
                0,
                comparison.rows_modified,
                round(comparison.rows_modified / orig.shape[0] * 100, 2),
            ],
            "Average Review Length": [
                round(orig["review_length"].mean(), 2),
                round(clean["review_length"].mean(), 2),
                round(
                    (orig["review_length"].mean() - clean["review_length"].mean())
                    / orig["review_length"].mean(),
                    2,
                )
                * 100,
            ],
            "Size (Mb)": [
                comparison.self_dataset_size,
                comparison.other_dataset_size,
                round(
                    (comparison.self_dataset_size - comparison.other_dataset_size)
                    / comparison.self_dataset_size
                    * 100,
                    2,
                ),
            ],
        }
        return pd.DataFrame.from_dict(
            data=comparison_dict,
            orient="index",
            columns=["Original", "Clean", "% Change"],
        )
