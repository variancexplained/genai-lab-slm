#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/analytics/eda.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Thursday December 19th 2024 04:48:42 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Exploratory Data Analysis Module"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from discover.analytics.analysis import Analysis
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class EDA(Analysis):
    """
    A class for analyzing and visualizing descriptive statistics and summaries of app review data.

    The `EDA` class encapsulates various methods and tools for conducting
    Exploratory Data Analysis (EDA) on app reviews, providing functionality for
    generating overviews, retrieving subsets of data, sampling, and plotting
    distributions, frequencies, and associations.

    Attributes:
        df (pd.DataFrame): The input DataFrame containing app review data.
        distribution (Distribution): An object for analyzing distributions of the data.
        describe (Describe): An object for generating descriptive statistics.
        freqdist (FreqDist): An object for computing frequency distributions.
        freqplot (FrequencyPlot): An object for plotting frequency distributions.
        freqdistplot (FreqDistPlot): An object for visualizing frequency distributions.
        distplot (DistributionPlot): An object for plotting data distributions.
        association (Association): An object for analyzing associations between features.
        assocplot (AssociationPlot): An object for plotting associations.
        multicount (MultivariateCount): An object for counting multivariate relationships.
        multimean (MultivariateStatisticsMean): An object for calculating means in multivariate statistics.
    """

    def __init__(self, df: pd.DataFrame) -> None:
        super().__init__(df=df)

    def distribution(self, x: str) -> plt.Axes:
        desc = self._df[x].describe().to_frame().T
        fig, (ax1, ax2) = plt.subplots(figsize=(12, 4), nrows=1, ncols=2)
        sns.kdeplot(data=self._df, x=x, ax=ax1)

        ax1r = ax1.twinx()
        sns.kdeplot(data=self._df, x=x, ax=ax1r, cumulative=True)
        sns.violinplot(data=self._df, x=x, ax=ax2)

        # Titles
        ax1.set_title(
            f"Probability Density and Cumulative Distribution of {x.replace('_', ' ').title()}"
        )
        ax2.set_title(f"Distribution of {x.replace('_', ' ').title()}")
        fig.suptitle(f"Distribution Analysis of {x.replace('_', ' ').title()}")

        plt.tight_layout()
        plt.show()
        return desc
