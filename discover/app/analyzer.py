#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/analyzer.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 11:07:32 am                                                #
# Modified   : Tuesday October 29th 2024 07:59:01 pm                                               #
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


# ------------------------------------------------------------------------------------------------ #
class Analyzer:
    """"""

    def __init__(self) -> None:
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

    def info(self, df: pd.DataFrame) -> None:
        self._overview = Overview(data=df)
        return self._overview.info()

    def overview(self, df: pd.DataFrame) -> None:
        n = df.shape[0]
        p = df.shape[1]
        n_auth = df["author"].nunique()
        n_auth_inf = df.loc[df["vote_count"] > 0].nunique()
        n_apps = df["app_id"].nunique()
        n_categories = df["category"].nunique()
        mem = df.memory_usage(deep=True)
        dt_first = df["date"].min()
        dt_last = df["date"].max()
        d = {
            "Number of Reviews": n,
            "Number of Authors": n_auth,
            "Number of Authors with Influence": n_auth_inf,
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
        return self._discribe

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
