#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/bivariate.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday October 20th 2024 05:43:16 pm                                                #
# Modified   : Tuesday October 29th 2024 08:01:19 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


import matplotlib.pyplot as plt
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
class Association(Analysis):
    def __call__(
        self,
        x: str = None,
        y: str = None,
        orient: str = "h",
        df: pd.DataFrame = None,
        hue: str = None,
        fill: bool = True,
        dodge: str = "auto",
        title: str = None,
    ) -> pd.DataFrame:

        fig, ax = plt.subplots(figsize=(6, 6))
        sns.barplot(
            data=df,
            x=x,
            y=y,
            orient=orient,
            hue=hue,
            fill=fill,
            ax=ax,
        )

        ax.set_title(title)

        plt.tight_layout()


# ------------------------------------------------------------------------------------------------ #
class AssociationPlot(Analysis):

    def __call__(
        self,
        x: str = None,
        y: str = None,
        orient: str = "h",
        df: pd.DataFrame = None,
        hue: str = None,
        fill: bool = True,
        dodge: str = "auto",
        title: str = None,
    ) -> pd.DataFrame:

        fig, ax = plt.subplots(figsize=(6, 6))
        sns.barplot(
            data=df,
            x=x,
            y=y,
            orient=orient,
            hue=hue,
            fill=fill,
            ax=ax,
        )

        ax.set_title(title)

        plt.tight_layout()
