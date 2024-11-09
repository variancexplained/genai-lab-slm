#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/eda.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Saturday November 9th 2024 05:36:33 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Analysis Class"""
from __future__ import annotations

import pandas as pd
from explorify.eda.overview import Overview

from discover.app.bivariate import Association, AssociationPlot
from discover.app.multivariate import MultivariateCount, MultivariateStatisticsMean
from discover.app.univariate import (
    Describe,
    Distribution,
    DistributionPlot,
    FreqDist,
    FreqDistPlot,
    FrequencyPlot,
)
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()

columns = [
    "id",
    "app_id",
    "app_name",
    "category",
    "author",
    "rating",
    "content",
    "vote_count",
    "vote_sum",
    "date",
    "enrichment_meta_review_age",
    "enrichment_meta_review_length",
    "enrichment_meta_review_month",
    "enrichment_meta_review_day_of_week",
    "enrichment_meta_review_hour",
    "enrichment_sentiment",
    "enrichment_sentiment_classification",
    "enrichment_tqa_score_final",
    "enrichment_pct_deviation_rating",
    "enrichment_pct_deviation_review_length",
    "enrichment_pct_deviation_sentiment",
    "enrichment_pct_deviation_tqa_score_final",
    "pos_n_nouns",
    "pos_n_verbs",
    "pos_n_adjectives",
    "pos_n_adverbs",
    "pos_p_nouns",
    "pos_p_verbs",
    "pos_p_adjectives",
    "pos_p_adverbs",
    "stats_char_count",
    "stats_unique_word_count",
    "stats_unique_word_proportion",
    "tqm_pos_count_score",
    "tqm_pos_diversity_score",
    "tqm_structural_complexity_score",
    "tqm_pos_intensity_score",
]


# ------------------------------------------------------------------------------------------------ #
class EDA:
    """"""

    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df[columns]
        self._df = self._df.convert_dtypes(convert_string=True)
        self._distribution = Distribution()
        self._describe = Describe()
        self._freqdist = FreqDist()
        self._freqplot = FrequencyPlot()
        self._freqdistplot = FreqDistPlot()
        self._distplot = DistributionPlot()
        self._association = Association()
        self._assocplot = AssociationPlot()
        self._multicount = MultivariateCount()
        self._multimean = MultivariateStatisticsMean()

    def info(self) -> None:
        self._overview = Overview(data=self._df)
        return self._overview.info()

    def overview(self) -> None:
        n = self._df.shape[0]
        p = self._df.shape[1]
        n_auth = self._df["author"].nunique()
        n_auth_inf = self._df.loc[self._df["vote_count"] > 0]["author"].nunique()
        n_repeat_auth = (self._df["author"].value_counts() > 1).sum()
        n_apps = self._df["app_id"].nunique()
        n_categories = self._df["category"].nunique()
        mem = self._df.memory_usage(deep=True).sum().sum()
        dt_first = self._df["date"].min()
        dt_last = self._df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Reviewrs": n_auth,
            "Number of Repeat Reviewers": n_repeat_auth,
            "Number of Influential Reviewers": n_auth_inf,
            "Number of Apps": n_apps,
            "Number of Categories": n_categories,
            "Features": p,
            "Memory Size (Mb)": round(mem / (1024 * 1024), 2),
            "Date of First Review": dt_first,
            "Date of Last Review": dt_last,
        }
        title = "AppVoCAI Dataset Overview"
        printer.print_dict(title=title, data=d)

    @property
    def distribution(self) -> Distribution:
        return self._distribution

    @property
    def describe(self) -> Describe:
        return self._describe

    @property
    def freqdist(self) -> FreqDist:
        return self._freqdist

    @property
    def freqplot(self) -> FrequencyPlot:
        return self._freqplot

    @property
    def freqdistplot(self) -> FreqDistPlot:
        return self._freqdistplot

    @property
    def distplot(self) -> DistributionPlot:
        return self._distplot

    @property
    def association(self) -> Association:
        return self._association

    @property
    def assocplot(self) -> AssociationPlot:
        return self._assocplot

    @property
    def multicount(self) -> MultivariateCount:
        return self._multicount

    @property
    def multimean(self) -> MultivariateStatisticsMean:
        return self._multimean
