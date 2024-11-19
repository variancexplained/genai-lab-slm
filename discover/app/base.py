#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/base.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Tuesday November 19th 2024 12:52:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Class"""
from __future__ import annotations

from abc import ABC
from typing import Callable

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from explorify.eda.visualize.visualizer import Visualizer

from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class Analysis(ABC):

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df
        self._visualizer = Visualizer()

    @property
    def visualize(self) -> Visualizer:
        return self._visualizer

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

    def select(
        self,
        n: int = 10,
        x: str = None,
        exclude: str = None,
        condition: Callable = None,
        sort_by: str = None,
        cols: list = None,
        ascending: bool = False,
    ) -> pd.DataFrame:
        df = self._df
        if exclude:
            df = df.loc[~df[exclude]]
        if x and condition:
            df = df.loc[df[x].apply(condition)]
        if sort_by:
            df = df.sort_values(sort_by, ascending=ascending)
        if cols:
            df = df[cols]
        if n:
            df = df.head(n=n)
        return df.reset_index(drop=True)

    def sample(
        self, n: int = 10, cols: list = None, random_state: int = None
    ) -> pd.DataFrame:
        n = min(n, len(self._df))
        df = self._df.sample(n=n, random_state=random_state)
        if cols:
            df = df[cols]
        if n == 1:
            printer.print_dataframe_as_dict(
                df=df, title=df["app_name"].values[0], list_index=0, text_col="content"
            )
        else:
            return df

    def show_review(self, review_id: str) -> None:
        df = self._df.loc[self._df["id"] == review_id]
        self._printer.print_dataframe_as_dict(
            df=df, title=df["app_name"].values[0], text_col="content"
        )
