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
# Modified   : Sunday November 24th 2024 06:59:26 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

import pandas as pd


@dataclass
class DataFrameComparison:
    rows_removed: int
    rows_added: int
    rows_modified: int
    original_dataset_rows: int
    original_dataset_size: int
    processed_dataset_rows: int
    processed_dataset_size: int


def compare_dataframes(
    original_df: pd.DataFrame, processed_df: pd.DataFrame
) -> DataFrameComparison:
    """
    Compares two DataFrames and returns a DataFrameComparison object with key metrics.

    Args:
        original_df (pd.DataFrame): The original DataFrame.
        processed_df (pd.DataFrame): The processed DataFrame.

    Returns:
        DataFrameComparison: A dataclass containing comparison metrics.
    """
    # Convert DataFrames to hashable tuples for row-wise comparison
    original_rows = {tuple(row) for row in original_df.to_numpy()}
    processed_rows = {tuple(row) for row in processed_df.to_numpy()}

    # Determine rows added, removed, and modified
    rows_removed = len(original_rows - processed_rows)
    rows_added = len(processed_rows - original_rows)

    # To detect modified rows, find intersecting rows and count any differences in the content
    rows_modified = len(original_rows.intersection(processed_rows)) - len(
        set(original_df.columns).intersection(set(processed_df.columns))
    )

    # Get size in bytes for datasets
    original_size = original_df.memory_usage(deep=True).sum()
    processed_size = processed_df.memory_usage(deep=True).sum()

    # Create the comparison object
    comparison = DataFrameComparison(
        rows_removed=rows_removed,
        rows_added=rows_added,
        rows_modified=rows_modified,
        original_dataset_rows=len(original_df),
        original_dataset_size=original_size,
        processed_dataset_rows=len(processed_df),
        processed_dataset_size=processed_size,
    )
    return comparison
