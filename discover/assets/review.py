#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/review.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Monday November 4th 2024 11:39:44 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Class"""
from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from explorify.eda.overview import Overview

from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
# ------------------------------------------------------------------------------------------------ #

cols1 = [
    "id",
    "app_id",
    "app_name",
    "category_id",
    "category",
    "author",
    "rating",
    "content",
    "vote_sum",
    "vote_count",
    "date",
]

cols2 = [
    "id",
    "app_id",
    "app_name",
    "category_id",
    "category",
    "author",
    "rating",
    "content",
    "vote_sum",
    "vote_count",
    "date",
    "eda_rating_avg_by_app_id",
    "eda_rating_deviation_by_app_id",
    "eda_review_length",
    "eda_review_length_avg_by_app_id",
    "eda_review_length_deviation_by_app_id",
    "eda_review_age",
    "eda_review_age_avg_by_app_id",
    "eda_review_age_deviation_by_app_id",
    "eda_rating_avg_by_category_id",
    "eda_rating_deviation_by_category_id",
    "eda_review_length",
    "eda_review_length_avg_by_category_id",
    "eda_review_length_deviation_by_category_id",
    "eda_review_age",
    "eda_review_age_avg_by_category_id",
    "eda_review_age_deviation_by_category_id",
]


# ------------------------------------------------------------------------------------------------ #
class Review:
    """"""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def info(self) -> None:
        if sum([col for col in self._df.columns if "eda_" in col]) > 0:
            self._overview = Overview(data=self._df[cols2])
        else:
            self._overview = Overview(data=self._df[cols1])
        return self._overview.info()

    def overview(self) -> None:
        n = self._df.shape[0]
        p = self._df.shape[1]
        n_auth = self._df["author"].nunique()
        n_apps = self._df["app_id"].nunique()
        n_categories = self._df["category"].nunique()
        mem = self._df.memory_usage(deep=True)
        dt_first = self._df["date"].min()
        dt_last = self._df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Authors": n_auth,
            "Number of Apps": n_apps,
            "Number of Categories": n_categories,
            "Features": p,
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        title = "AppVoCAI Dataset Overview"
        printer.print_dict(title=title, data=d)

    def categories(
        self,
        x: str = None,
        y: str = None,
        stat: str = "count",
        topn: int = None,
        dodge: str = "auto",
        title: str = None,
        threshold: int = 20,
        ax: plt.Axes = None,
    ) -> plt.Axes:
        var = x if x else y
        orient = "v" if x else "h"
        figsize = (12, 4) if orient == "v" else (4, 6)
        fig, ax = plt.subplots(figsize=figsize)

        df = self._df
        counts = df[var].value_counts(ascending=False)
        stats = counts.describe().to_frame().T

        if len(counts) > threshold or topn:
            df = self._get_most_frequent(df=self._df, var=var, counts=counts, topn=topn)

        total = len(df)  # Used to compute % of counts

        sns.countplot(
            data=df,
            x=x,
            y=y,
            stat=stat,
            dodge=dodge,
            ax=ax,
            order=df[var].value_counts().index,
        )

        if title:
            ax.set_title(title)

        if orient == "v":
            for p in ax.patches:
                x = p.get_bbox().get_points()[:, 0]
                y = p.get_bbox().get_points()[1, 1]
                ax.annotate(
                    text=f"{round(y,0)}\n({round(y/total*100,1)}%)",
                    xy=(x.mean(), y),
                    ha="center",
                    va="bottom",
                )
        else:
            for p in ax.patches:
                x = p.get_x() + p.get_width()
                y = p.get_y() + p.get_height() / 2
                ax.annotate(
                    text=f"{round(p.get_width(),0)} ({round(p.get_width()/total*100,1)}%)",
                    xy=(x, y),
                    va="center",
                )
        return stats

    def _get_most_frequent(
        self, df: pd.DataFrame, var: str, counts: pd.Series, topn: int = 10
    ) -> plt.Axes:

        counts = counts.to_frame()
        counts = counts.iloc[0:topn].reset_index()
        return df.loc[df[var].isin(counts[var])]
