#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/utils/data/compare.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 24th 2024 06:56:09 pm                                               #
# Modified   : Sunday November 24th 2024 09:07:42 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

import pandas as pd

from discover.core.data_class import DataClass


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataFrameComparison(DataClass):
    rows_removed: int
    rows_added: int
    rows_modified: int
    self_dataset_rows: int
    self_dataset_size: int
    other_dataset_rows: int
    other_dataset_size: int


def compare_dataframes(
    self_df: pd.DataFrame, other_df: pd.DataFrame, cols: list = None
) -> DataFrameComparison:
    """
    Compares two DataFrames and returns a DataFrameComparison object with key metrics.

    Args:
        self_df (pd.DataFrame): The self DataFrame.
        other_df (pd.DataFrame): The other DataFrame.

    Returns:
        DataFrameComparison: A dataclass containing comparison metrics.
    """
    if cols:
        self_df = self_df[cols]
        other_df = other_df[cols]
    # Determine rows removed
    rows_removed = max(0, self_df.shape[0] - other_df.shape[0])
    rows_added = max(0, other_df.shape[0] - self_df.shape[0])

    # Get common ids
    common_ids = list(set(self_df["id"]).intersection(set(other_df["id"])))

    # Subset by self and other by common ids and sort.
    self_common = (
        self_df.loc[self_df["id"].isin(common_ids)].sort_values(by="id").reset_index()
    )
    other_common = (
        other_df.loc[other_df["id"].isin(common_ids)].sort_values(by="id").reset_index()
    )

    # Compare the two dataframes to determine changes.
    comparison = self_common.compare(
        other_common,
        keep_shape=False,
        keep_equal=True,
        result_names=("self", "clean"),
    )

    # To detect modified rows, find intersecting rows and count any differences in the content
    rows_modified = comparison.shape[0]

    # Get size in bytes for datasets
    self_size = self_df.memory_usage(deep=True).sum()
    other_size = other_df.memory_usage(deep=True).sum()

    # Create the comparison object
    comparison = DataFrameComparison(
        rows_removed=rows_removed,
        rows_added=rows_added,
        rows_modified=rows_modified,
        self_dataset_rows=len(self_df),
        self_dataset_size=self_size,
        other_dataset_rows=len(other_df),
        other_dataset_size=other_size,
    )
    return comparison
