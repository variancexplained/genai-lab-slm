#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/multivariate.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday October 20th 2024 05:43:16 pm                                                #
# Modified   : Tuesday October 29th 2024 07:47:34 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from typing import Union

import pandas as pd
import seaborn as sns

from discover.app.base import Analysis
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
printer = Printer()
# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class MultivariateCount(Analysis):

    def __call__(
        self,
        df: pd.DataFrame,
        cols: Union[str, list],
        labels: Union[str, list] = None,
        groupby: str = None,
    ):
        data = df[cols]
        if groupby:
            data = data.groupby(by=groupby).nunique()
        if labels:
            data.columns = labels
        return data


# ------------------------------------------------------------------------------------------------ #
class MultivariateStatisticsMean(Analysis):

    def __call__(
        self,
        df: pd.DataFrame,
        cols: Union[str, list],
        labels: Union[str, list] = None,
        groupby: str = None,
    ):
        data = df[cols]
        if groupby:
            data = data.groupby(by=groupby).mean()
        else:
            data.mean()
        return data
