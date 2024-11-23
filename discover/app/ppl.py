#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/ppl.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday November 19th 2024 06:25:51 am                                              #
# Modified   : Friday November 22nd 2024 02:58:17 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Perplexity Analysis Module"""
from __future__ import annotations

import pandas as pd

from discover.app.analysis import Analysis
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()


# ------------------------------------------------------------------------------------------------ #
class PerplexityAnalyzer(Analysis):
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._perplexity_col = [
            col for col in self._df.columns if col.startswith("pa_")
        ][0]

    def max_perplexity_by_percentile(self, percentiles: list, cols: list = None):
        """Iterates through given percentiles of perplexity values and returns
        the row with the maximum perplexity for each percentile.

        Args:
            percentiles (list): A list of percentiles (0-100) to evaluate.

        Returns:
            pd.DataFrame: A DataFrame containing rows with the maximum perplexity
                        for each percentile.
        """
        results = []

        # Sort the data by perplexity in ascending order
        sorted_data = self._df.sort_values(by=self._perplexity_col, ascending=True)

        for p in percentiles:
            # Calculate the threshold for the current percentile
            threshold = sorted_data[self._perplexity_col].quantile(p / 100.0)

            # Filter rows up to the current percentile threshold
            subset = sorted_data[sorted_data[self._perplexity_col] <= threshold]

            # Add the thresold to the subset
            subset["percentile"] = p

            # Select the row with the maximum complexity (perplexity) in this subset
            max_row = subset.iloc[-1]  # The last row will have the max perplexity
            results.append(max_row)

        # Return the results as a new DataFrame
        df = pd.DataFrame(results)
        if cols:
            df = df[cols]
        return df
