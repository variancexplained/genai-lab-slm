#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/tqa.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 29th 2024 08:20:00 pm                                               #
# Modified   : Tuesday November 19th 2024 01:07:06 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Module"""
import logging

import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import ks_2samp

from discover.app.base import Analysis

# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
sns.set_palette("Blues_r")


# ------------------------------------------------------------------------------------------------ #
class TextQualityAnalysis(Analysis):
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def plot_thresholds(
        self, min: float = 0.1, max: float = 0.9, num: int = 20, metrics: list = None
    ) -> None:
        logging.getLogger("matplotlib").setLevel(logging.ERROR)
        import matplotlib.pyplot as plt

        thresholds = np.linspace(
            self._df["tqa_score"].quantile(min),
            self._df["tqa_score"].quantile(max),
            num=num,
        )

        metrics = metrics or [col for col in self._df.columns if col.startswith("tqm")]

        t = []
        m = []
        v = []
        n = []

        for threshold in thresholds:
            # Filter reviews with tqa_score above the current threshold
            filtered_data = self._df.loc[self._df["tqa_score"] >= threshold]

            # Calculate mean values for each metric
            for metric in metrics:
                v.append(filtered_data[metric].mean())
                m.append(metric)
                t.append(threshold)
                n.append(len(filtered_data[metric]))

        # Convert results to a DataFrame
        data = {"Threshold": t, "Metric": m, "Value": v, "Number of Observations": n}
        threshold_df = pd.DataFrame(data)

        fig, ax = plt.subplots(figsize=(12, 4))
        ax2 = ax.twinx()
        sns.lineplot(data=threshold_df, x="Threshold", y="Value", hue="Metric", ax=ax)
        sns.lineplot(
            data=threshold_df,
            x="Threshold",
            y="Number of Observations",
            ax=ax2,
            color="coral",
            legend=True,
        )
        plt.title(label="Text Quality Threshold Analysis")
        plt.tight_layout()


# ------------------------------------------------------------------------------------------------ #
class DatasetEvaluation:
    def __init__(self, df: pd.DataFrame) -> None:
        self._df = df

    def histogram(
        self,
        column: str,
        min: float = 0.1,
        max: float = 0.9,
        num: int = 6,
        cumulative: bool = False,
    ) -> None:
        logging.getLogger("matplotlib").setLevel(logging.ERROR)
        import matplotlib.pyplot as plt

        column_label = column.replace("_", " ")

        thresholds = np.linspace(
            self._df["tqa_score"].quantile(min),
            self._df["tqa_score"].quantile(max),
            num=num,
        )
        # Set aside the full dataset
        df1 = self._df
        # Obtain a canvas for 2 plots
        fig, ax = plt.subplots(figsize=(12, 4))
        # Plot the full dataset distribution
        sns.kdeplot(
            data=df1,
            x=column,
            color="black",
            label="Full Dataset",
            ax=ax,
            cumulative=cumulative,
            linewidth=2,
            linestyle="--",
            alpha=0.8,
        )
        # Define a color palette for the thresholds
        colors = sns.color_palette("viridis", len(thresholds))
        # Plot each thresholded subset distribution
        for i, threshold in enumerate(thresholds):
            df2 = self._df.loc[self._df["tqa_score"] >= threshold]
            sns.kdeplot(
                data=df2,
                x=column,
                label=f"Threshold > {round(threshold,2)}",
                color=colors[i],
                alpha=0.7,
                cumulative=cumulative,
                ax=ax,
            )

        # Set titles and labels
        ax.set_title(f"Distribution of {column_label.capitalize()} Across Thresholds")
        plt.legend()
        plt.tight_layout()
        plt.show()

    def kstest(
        self, column: str, min: float = 0.1, max: float = 0.9, num: int = 6
    ) -> None:
        logging.getLogger("matplotlib").setLevel(logging.ERROR)
        import matplotlib.pyplot as plt

        column_label = column.replace("_", " ").capitalize()

        thresholds = np.linspace(
            self._df["tqa_score"].quantile(min),
            self._df["tqa_score"].quantile(max),
            num=num,
        )
        # Set aside the full dataset
        df1 = self._df
        # Obtain a canvas for 2 plots
        fig, ax = plt.subplots(figsize=(12, 4))

        t = []
        ks = []
        # Plot each thresholded subset distribution
        for i, threshold in enumerate(thresholds):
            df2 = self._df.loc[self._df["tqa_score"] >= threshold]
            ks_stat = ks_2samp(df1[column], df2[column])
            t.append(str(round(threshold, 2)))
            ks.append(float(ks_stat.statistic))

        ks_df = pd.DataFrame({"Threshold": t, "KS-Statistic": ks})
        ks_df["Threshold"] = ks_df["Threshold"].astype("category")
        sns.barplot(
            data=ks_df,
            x="Threshold",
            y="KS-Statistic",
            ax=ax,
            palette="viridis",
        )
        # Set titles and labels
        ax.set_title(
            f"Kolmogorov Statistic of {column_label} Goodness of Fit Across Thresholds"
        )
        ax.axhline(
            0.05,
            color="red",
            linestyle="--",
            label="Practical Similarity Threshold (0.05)",
        )
        plt.tight_layout()
        plt.legend()
        plt.show()

    def violin(
        self,
        column: str,
        min: float = 0.1,
        max: float = 0.9,
        num: int = 6,
        cumulative: bool = False,
    ) -> None:
        logging.getLogger("matplotlib").setLevel(logging.ERROR)
        import matplotlib.pyplot as plt

        column_label = column.replace("_", " ").capitalize()

        thresholds = np.linspace(
            self._df["tqa_score"].quantile(min),
            self._df["tqa_score"].quantile(max),
            num=num,
        )
        # Obtain a canvas for 2 plots
        fig, ax = plt.subplots(figsize=(12, 4))
        # Set aside the full dataset
        df1 = self._df
        df1["Dataset"] = "Full"

        # Filter the dataset by thresholds
        for i, threshold in enumerate(thresholds):
            df2 = self._df.loc[self._df["tqa_score"] >= threshold]
            df2["Dataset"] = f"Threshold > {round(threshold,2)}"
            df1 = pd.concat([df1, df2])

        sns.violinplot(
            data=df1,
            y=column,
            palette="viridis",
            hue="Dataset",
            alpha=0.7,
            ax=ax,
        )

        # Set titles and labels
        ax.set_title(f"Distribution of {column_label} Across Thresholds")
        plt.legend()
        plt.tight_layout()
        plt.show()
