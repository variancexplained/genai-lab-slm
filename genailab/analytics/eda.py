#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/analytics/eda.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Exploratory Data Analysis Module"""
from __future__ import annotations

from typing import Union

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from genailab.analytics.base import Analysis
from genailab.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
sns.set_style("whitegrid")
sns.set_palette("Blues_r")

# ------------------------------------------------------------------------------------------------ #
class EDA(Analysis):

    def __init__(self, df: Union[pd.DataFrame, pd.core.frame.DataFrame]) -> None:
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
