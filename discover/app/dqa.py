#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/app/dqa.py                                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 10:43:56 am                                                #
# Modified   : Saturday November 16th 2024 07:36:52 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""

from typing import Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from explorify.eda.visualize.visualizer import Visualizer
from pandarallel import pandarallel

from discover.app.base import Analysis
from discover.assets.idgen import AssetIDGen
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
viz = Visualizer()
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=18, verbose=False)


# ------------------------------------------------------------------------------------------------ #
#                         DATA QUAAITY ANALYSIS SERVICE                                            #
# ------------------------------------------------------------------------------------------------ #
class DQA(Analysis):
    def __init__(
        self,
        name: str = "review",
        config_reader_cls: type[AppConfigReader] = AppConfigReader,
    ) -> None:
        super().__init__()
        self._config = config_reader_cls().get_config(section="dqa", namespace=True)
        # Obtain the dataset asset id
        asset_id = AssetIDGen().get_asset_id(
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=StageDef.TQA,
            name=name,
        )
        # Load the dataset
        self._df = self.load_data(asset_id=asset_id).content

        # Measures
        self._completeness = None
        self._validity = None
        self._uniqueness = None
        self._balance = None
        self._accuracy = None
        self._privacy = None
        self._text_quality = None
        self._scores = None

        # Extract the columns containing the binary indicators of defects
        self._base_cols = ["id", "app_name", "content"]
        self._cols = [col for col in self._df.columns if col.startswith("tqd")]
        self._noise_cols = [
            col
            for col in self._df.columns
            if (
                col.startswith("tqd_")
                and "email" not in col
                and "url" not in col
                and "phone" not in col
            )
        ]
        self._privacy_cols = [
            col
            for col in self._df.columns
            if ("email" in col or "url" in col or "phone" in col)
        ]

    @property
    def completeness(self) -> float:
        if not self._completeness:
            self._completeness = self._compute_completeness()
        return self._completeness

    @property
    def validity(self) -> float:
        if not self._validity:
            self._validity = self._compute_validity()
        return self._validity

    @property
    def uniqueness(self) -> float:
        if not self._uniqueness:
            self._uniqueness = self._compute_uniqueness()
        return self._uniqueness

    @property
    def balance(self) -> float:
        if not self._balance:
            self._balance = self._compute_balance()
        return self._balance

    @property
    def accuracy(self) -> float:
        if not self._accuracy:
            self._accuracy = self._compute_accuracy()
        return self._accuracy

    @property
    def privacy(self) -> float:
        if not self._privacy:
            self._privacy = self._compute_privacy()
        return self._privacy

    @property
    def text_quality(self) -> float:
        if not self._text_quality:
            self._text_quality = self._compute_text_quality()
        return self._text_quality

    @property
    def quality(self) -> float:
        return self._compute_quality()

    @property
    def quality_scores(self) -> pd.DataFrame:
        if self._scores is None:
            self._scores = self._summarize_quality()
        return self._scores

    def _summarize_quality(self) -> pd.DataFrame:
        d = {
            "Dimension": [
                "Completeness",
                "Validity",
                "Uniqueness",
                "Balance",
                "Accuracy",
                "Data Privacy",
                "Text Quality",
            ],
            "Score": [
                self.completeness,
                self.validity,
                self.uniqueness,
                self.balance,
                self.accuracy,
                self.privacy,
                self.text_quality,
            ],
        }
        self._scores = pd.DataFrame(data=d)
        return self._scores

    def summarize_noise(self) -> pd.DataFrame:
        return self._compute_frequency_distribution(cols=self._noise_cols)

    def summarize_privacy(self) -> pd.DataFrame:
        return self._compute_frequency_distribution(cols=self._privacy_cols)

    def plot_quality(self) -> None:
        viz.barplot(
            data=self.quality_scores,
            x="Dimension",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nQuality Score: {round(self.quality,3)}",
        )

    def plot_review_length_validity(self) -> pd.DataFrame:
        outliers = self._df.loc[self._df["ing_review_length_outlier"]]["review_length"]
        viz.violinplot(x=outliers, title="Distribution of Review Length Outliers")
        return outliers.describe().to_frame().T

    def plot_perplexity_validity(self) -> pd.DataFrame:
        outliers = self._df.loc[self._df["an_perplexity_outlier"]]["an_perplexity"]
        viz.violinplot(x=outliers, title="Distribution of Perplexity Outliers")
        return outliers.describe().to_frame().T

    def plot_balance(self) -> pd.DataFrame:
        viz.countplot(
            data=self._df,
            x="an_sentiment",
            title=f"Distribution of Sentiment Classification\nClass Balance Score: {round(self.balance,2)}",
        )
        return self._df["an_sentiment"].describe().to_frame().T

    def plot_privacy(self) -> None:
        privacy = self.summarize_privacy()
        viz.barplot(
            data=privacy,
            y="Defect",
            x="%",
            title="Personally Identifiable Information (PII)",
        )

    def plot_text_quality(self) -> pd.DataFrame:
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 4))
        viz.kdeplot(data=self._df, x="tqa_score", ax=axes[0])
        viz.violinplot(data=self._df, x="tqa_score", ax=axes[1])
        fig.suptitle("Distribution of Text Quality Scores")
        return self._df["tqa_score"].describe().to_frame().T

    def plot_noise(self) -> None:
        noise = self.summarize_noise()
        viz.barplot(data=noise, y="Defect", x="%", title="Noise Density")

    def get_defects(
        self,
        defect: str,
        n: int = 10,
        sort_by: str = None,
        cols: Optional[Union[list, str]] = None,
        ascending: bool = False,
        random_state: int = None,
    ) -> pd.DataFrame:
        # Obtain the defect indicator column
        defect_cols = self._get_columns(substring=defect)
        # Apply the filter
        df = self._df.loc[self._df[defect_cols[0]]]
        # Update n for filtered data
        n = min(n, len(df))
        # Sample if sort_by is None. Rationale is that sorting implies all data, rather than a sample
        if not sort_by:
            df = df.sample(n=n, random_state=random_state)
        # Sort data if requested
        else:
            df = df.sort_values(by=sort_by, ascending=ascending)
        # If no columns were specified, return base columns
        if cols:
            if isinstance(cols, list):
                return df[cols]
            elif isinstance(cols, str):
                cols = self._get_columns(cols)
                if cols:
                    base_cols = self._base_cols
                    base_cols.extend(cols)
                    return df[base_cols]
                else:
                    return df
            else:
                raise TypeError(
                    f"Invalid cols value in get_defect. Expected str or list, encountered {type(cols)} "
                )
        else:
            return df

    def subset_df(self, n: int = 10, random_state: int = None) -> pd.DataFrame:
        n = min(n, len(self._df))
        return self._df.sample(n=n, random_state=random_state)

    def _compute_validity(self) -> float:
        N = self._df.shape[0]
        valid_ratings = (
            N
            - self._df[
                ~self._df["rating"].parallel_apply(
                    lambda x: isinstance(x, int) and 1 <= x <= 5
                )
            ].shape[0]
        )
        review_length_non_outliers = N - (self._df["ing_review_length_outlier"].sum())
        perplexity_non_outliers = N - (+self._df["an_perplexity_outlier"].sum())
        return (
            ((valid_ratings / N) * self._config.rating_validity_weight)
            + (
                (review_length_non_outliers / N)
                * self._config.review_length_outlier_validity_weight
            )
            + (
                (perplexity_non_outliers / N)
                * self._config.perplexity_outlier_validity_weight
            )
        )

    def _compute_uniqueness(self) -> float:
        N = self._df.shape[0]
        n_unique_reviews = self._df[["app_id", "content"]].drop_duplicates().shape[0]
        n_unique_ids = self._df["id"].nunique()
        return ((n_unique_reviews / N) * self._config.review_uniqueness_weight) + (
            (n_unique_ids / N) * self._config.id_uniqueness_weight
        )

    def _compute_balance(self) -> float:
        N = self._df.shape[0]
        sentiments = self._df["an_sentiment"].value_counts()
        mean_sentiment = np.mean(sentiments)
        balance = 1 - (np.sum(np.abs(sentiments - mean_sentiment)) / N)
        return balance

    def _compute_accuracy(self) -> float:
        N = self._df.shape[0]
        return 1 - (self._df[self._noise_cols].any(axis=1).sum() / N)

    def _compute_privacy(self) -> float:
        N = self._df.shape[0]
        return 1 - (self._df[self._privacy_cols].any(axis=1).sum() / N)

    def _compute_text_quality(self) -> float:
        return self._df["tqa_score"].mean()

    def _compute_quality(self) -> float:
        return (
            self.completeness * self._config.completeness_weight
            + self.validity * self._config.validity_weight
            + self.uniqueness * self._config.uniqueness_weight
            + self.balance * self._config.balance_weight
            + self.accuracy * self._config.accuracy_weight
            + self.privacy * self._config.privacy_weight
            + self.text_quality * self._config.text_quality_weight
        )

    def _compute_frequency_distribution(self, cols: list) -> pd.DataFrame:
        # Extract the dqa data
        dqa = self._df[cols]
        # Sum the indicator variables
        df = dqa.sum(axis=0)
        # Create a dataframe of counts
        df = pd.DataFrame(df, columns=["n"])
        # Add relative counts
        df["%"] = round(dqa.sum(axis=0) / self._df.shape[0] * 100, 2)
        # Reset the index and expose the defect column
        df = df.reset_index(names=["Defect"])
        # Convert columns to labels
        df["Defect"] = df["Defect"].apply(self._convert_labels)
        # Select and order columns
        df = df[["Defect", "n", "%"]]
        return df.sort_values(by="n", ascending=False).reset_index(drop=True)

    def _get_columns(self, substring) -> Optional[str]:
        return [col for col in self._cols if substring in col]

    def _convert_labels(self, txt) -> str:
        """Converts column names to Title case labels."""
        txt = txt.replace("tqd_", "")
        txt = txt.replace("_", " ")
        txt = txt.title()
        txt = "Contains " + txt
        return txt

    def _compute_completeness(self) -> float:
        complete_rows = self._df.dropna().shape[0]
        total_rows = self._df.shape[0]
        completeness = complete_rows / total_rows
        return completeness
