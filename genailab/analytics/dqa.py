#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/analytics/dqa.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 10:43:56 am                                                #
# Modified   : Tuesday January 28th 2025 12:35:15 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Type, Union

import pandas as pd
from explorify.eda.visualize.visualizer import Visualizer
from genailab.analytics.base import Analysis
from genailab.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
viz = Visualizer()

# ------------------------------------------------------------------------------------------------ #
if TYPE_CHECKING:
    from genailab.asset.dataset.dataset import Dataset


# ------------------------------------------------------------------------------------------------ #
#                         DATA QUALITY ANALYSIS SERVICE                                            #
# ------------------------------------------------------------------------------------------------ #
class DQA(Analysis):

    def __init__(
        self,
        dataset: "Dataset",
        config_reader_cls: Type[AppConfigReader] = AppConfigReader,
    ) -> None:
        df = dataset.dataframe
        self._dataset = dataset

        super().__init__(df=df)
        self._config_reader = config_reader_cls()
        self._config = self._config_reader.get_config(section="dqa", namespace=True)
        # Nobs
        self._N = self._df.shape[0]
        # Total quality score
        self._quality_score = None
        # Completeness Measures
        self._completeness = None

        # Validity Measures
        self._validity = None
        self._rating_validity = None
        self._category_validity = None
        self._date_validity = None
        self._review_validity = None

        # Relevance Measures
        self._relevance = None
        self._language_relevance = None
        self._review_length_relevance = None

        # Uniqueness Measures
        self._uniqueness = None
        self._row_uniqueness = None
        self._review_id_uniqueness = None
        self._review_uniqueness = None
        self._duplicate_rows = None
        self._duplicate_review_ids = None
        self._duplicate_reviews = None

        # Privacy Measures
        self._privacy = None

    # -------------------------------------------------------------------------------------------- #
    #                                    DATASET                                                   #
    # -------------------------------------------------------------------------------------------- #
    @property
    def dataset(self) -> "Dataset":
        return self._dataset

    # -------------------------------------------------------------------------------------------- #
    #                                 QUALITY SCORE                                                #
    # -------------------------------------------------------------------------------------------- #
    @property
    def quality_score(self) -> float:
        if not self._quality_score:
            self._quality_score = self._compute_quality_score()
        return self._quality_score

    # -------------------------------------------------------------------------------------------- #
    #                                  COMPLETENESS                                                #
    # -------------------------------------------------------------------------------------------- #
    @property
    def completeness(self) -> float:
        if not self._completeness:
            self._completeness = self._compute_completeness()
        return self._completeness


    # -------------------------------------------------------------------------------------------- #
    #                                     VALIDITY                                                 #
    # -------------------------------------------------------------------------------------------- #
    @property
    def validity(self) -> float:
        if not self._validity:
            self._validity = self._compute_validity()
        return self._validity

    @property
    def rating_validity(self) -> float:
        if not self._rating_validity:
            self._rating_validity = self._compute_rating_validity()
        return self._rating_validity

    @property
    def category_validity(self) -> float:
        if not self._category_validity:
            self._category_validity = self._compute_category_validity()
        return self._category_validity

    @property
    def date_validity(self) -> float:
        if not self._date_validity:
            self._date_validity = self._compute_review_date_validity()
        return self._date_validity

    @property
    def review_validity(self) -> float:
        if not self._review_validity:
            self._review_validity = self._compute_review_validity()
        return self._review_validity

    @property
    def uniqueness(self) -> float:
        if not self._uniqueness:
            self._uniqueness = self._compute_uniqueness()
        return self._uniqueness

    @property
    def row_uniqueness(self) -> float:
        if not self._row_uniqueness:
            self._row_uniqueness = self._compute_row_uniqueness()
        return self._row_uniqueness

    @property
    def duplicate_rows(self) -> int:
        if not self._duplicate_rows:
            self._duplicate_rows = self._compute_duplicate_rows()
        return self._duplicate_rows

    @property
    def review_id_uniqueness(self) -> float:
        if not self._review_id_uniqueness:
            self._review_id_uniqueness = self._compute_review_id_uniqueness()
        return self._review_id_uniqueness

    @property
    def duplicate_review_ids(self) -> int:
        if not self._duplicate_review_ids:
            self._duplicate_review_ids = self._compute_duplicate_review_ids()
        return self._duplicate_review_ids

    @property
    def review_uniqueness(self) -> float:
        if not self._review_uniqueness:
            self._review_uniqueness = self._compute_review_uniqueness()
        return self._review_uniqueness

    @property
    def duplicate_reviews(self) -> int:
        if not self._duplicate_reviews:
            self._duplicate_reviews = self._compute_duplicate_reviews()
        return self._duplicate_reviews

    @property
    def relevance(self) -> float:
        if not self._relevance:
            self._relevance = self._compute_relevance()
        return self._relevance

    @property
    def language_relevance(self) -> float:
        if not self._language_relevance:
            self._language_relevance = self._compute_language_relevance()
        return self._language_relevance

    @property
    def review_length_relevance(self) -> float:
        if not self._review_length_relevance:
            self._review_length_relevance = self._compute_review_length_relevance()
        return self._review_length_relevance

    @property
    def privacy(self) -> float:
        if not self._privacy:
            self._privacy = self._compute_privacy()
        return self._privacy

    # -------------------------------------------------------------------------------------------- #
    #                                 QUALITY SCORE                                                #
    # -------------------------------------------------------------------------------------------- #
    def analyze_quality(self, plot: bool = True) -> pd.DataFrame:
        df = self.summarize_quality()
        if plot:
            self.plot_quality(df=df)
        return df

    def summarize_anomalies(self) -> pd.DataFrame:
        cols = list(self._df.columns[self._df.columns.str.startswith("dqa")])
        return self._compute_frequency_distribution(cols=cols)

    def summarize_quality(self) -> pd.DataFrame:
        d = {
            "Dimension": [
                "Completeness",
                "Validity",
                "Relevance",
                "Uniqueness",
                "Privacy",
            ],
            "Score": [
                self.completeness,
                self.validity,
                self.relevance,
                self.uniqueness,
                self.privacy,
            ],
        }
        return pd.DataFrame(data=d)

    def _compute_quality_score(self) -> float:
        self._quality_score = (
            self._config.dqs.weights.completeness * self.completeness
            + self._config.dqs.weights.validity * self.validity
            + self._config.dqs.weights.relevance * self.relevance
            + self._config.dqs.weights.uniqueness * self.uniqueness
            + self._config.dqs.weights.privacy * self.privacy
        )
        return self._quality_score

    # -------------------------------------------------------------------------------------------- #
    #                                  COMPLETENESS                                                #
    # -------------------------------------------------------------------------------------------- #
    def _compute_completeness(self) -> float:
        self._completeness = self._df.dropna().shape[0] / self._N
        return self._completeness


    # -------------------------------------------------------------------------------------------- #
    #                                     VALIDITY                                                 #
    # -------------------------------------------------------------------------------------------- #
    def analyze_validity(self) -> pd.DataFrame:
        df = self.summarize_validity()
        self.plot_validity(df=df)
        return df

    def analyze_review_validity(self) -> pd.DataFrame:
        df = self.summarize_review_validity()
        self.plot_review_validity(df=df)
        return df

    def summarize_validity(self) -> pd.DataFrame:
        d = {
            "Component": [
                "Rating Validity",
                "Category Validity",
                "Review Date Validity",
                "Review Validity",
            ],
            "Score": [
                self.rating_validity,
                self.category_validity,
                self._review_date_validity,
                self.review_validity,
            ],
        }
        return pd.DataFrame(d).reset_index(drop=True)

    def summarize_review_validity(self) -> pd.DataFrame:
        columns = self._config.validity.columns.review_validity
        df = pd.DataFrame(
            {
                "Anomaly": self._convert_labels(col),
                "Count": self._df.loc[self._df[col]].shape[0],
                "%": self._df.loc[self._df[col]].shape[0] / self._df.shape[0],
            }
            for col in columns
        )
        return (
            df.loc[df["Count"] > 0]
            .sort_values(by="%", ascending=False)
            .reset_index(drop=True)
        )

    def _compute_validity(self) -> pd.DataFrame:
        self._validity = (
            self._config.validity.weights.rating * self.rating_validity
            + self._config.validity.weights.category * self.category_validity
            + self._config.validity.weights.review * self.review_validity
            + self._config.validity.weights.date * self.date_validity
        )
        return self._validity

    def _compute_rating_validity(self) -> float:
        column = self._config.validity.columns.rating_validity
        self._rating_validity = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._rating_validity

    def _compute_category_validity(self) -> float:
        column = self._config.validity.columns.category_validity
        self._category_validity = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._category_validity

    def _compute_review_date_validity(self) -> float:
        column = self._config.validity.columns.date_validity
        self._review_date_validity = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._review_date_validity

    def _compute_review_validity(self) -> float:
        # Retrieve the list of columns to evaluate from the configuration
        columns = self._config.validity.columns.review_validity

        # Calculate rows where all specified columns are False
        valid_rows = self._df.loc[~self._df[columns].any(axis=1)]

        # Compute the proportion of valid rows
        self._review_validity = valid_rows.shape[0] / self._N

        return self._review_validity

    # -------------------------------------------------------------------------------------------- #
    #                                    RELEVANCE                                                 #
    # -------------------------------------------------------------------------------------------- #
    def analyze_relevance(self) -> pd.DataFrame:
        df = self.summarize_relevance()
        self.plot_relevance(df=df)
        return df

    def summarize_relevance(self) -> pd.DataFrame:
        d = {
            "Component": ["Language Relevance", "Review Length Relevance"],
            "Score": [self.language_relevance, self.review_length_relevance],
        }
        return pd.DataFrame(d).reset_index(drop=True)

    def _compute_relevance(self) -> float:
        self._relevance = (
            self._config.relevance.weights.language * self.language_relevance
            + self._config.relevance.weights.review_length
            * self.review_length_relevance
        )
        return self._relevance

    def _compute_language_relevance(self) -> float:
        # Retrieve the list of columns to evaluate from the configuration
        columns = self._config.relevance.columns.language

        # Calculate rows where all specified columns are False
        valid_rows = self._df.loc[~self._df[columns].any(axis=1)]

        # Compute the proportion of valid rows
        self._language_relevance = valid_rows.shape[0] / self._N

        return self._language_relevance

    def _compute_review_length_relevance(self) -> float:
        # Retrieve the list of columns to evaluate from the configuration
        column = self._config.relevance.columns.review_length

        # Calculate rows where all specified columns are False
        valid_rows = self._df.loc[~self._df[column]]

        # Compute the proportion of valid rows
        self._review_length_relevance = valid_rows.shape[0] / self._N

        return self._review_length_relevance

    # -------------------------------------------------------------------------------------------- #
    #                                    UNIQUENESS                                                #
    # -------------------------------------------------------------------------------------------- #
    def analyze_uniqueness(self) -> pd.DataFrame:
        df = self.summarize_uniqueness()
        self.plot_uniqueness(df=df)
        return df

    def summarize_uniqueness(self) -> pd.DataFrame:
        d = {
            "Component": [
                "Row Uniqueness",
                "Review Id Uniqueness",
                "Review Uniqueness",
            ],
            "Duplicates": [
                self.duplicate_rows,
                self.duplicate_review_ids,
                self.duplicate_reviews,
            ],
            "Score": [
                self.row_uniqueness,
                self.review_id_uniqueness,
                self.review_uniqueness,
            ],
        }
        return pd.DataFrame(d).reset_index(drop=True)

    def _compute_uniqueness(self) -> float:
        self._uniqueness = (
            self._config.uniqueness.weights.row * self.row_uniqueness
            + self._config.uniqueness.weights.review_id * self.review_id_uniqueness
            + self._config.uniqueness.weights.review * self.review_uniqueness
        )
        return self._uniqueness

    def _compute_row_uniqueness(self) -> float:
        column = self._config.uniqueness.columns.row
        self._row_uniqueness = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._row_uniqueness

    def _compute_duplicate_rows(self) -> int:
        column = self._config.uniqueness.columns.row
        self._duplicate_rows = self._df.loc[self._df[column]].shape[0]
        return self._duplicate_rows

    def _compute_review_id_uniqueness(self) -> float:
        column = self._config.uniqueness.columns.review_id
        self._review_id_uniqueness = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._review_id_uniqueness

    def _compute_duplicate_review_ids(self) -> int:
        column = self._config.uniqueness.columns.review_id
        self._duplicate_review_ids = self._df.loc[self._df[column]].shape[0]
        return self._duplicate_review_ids

    def _compute_review_uniqueness(self) -> float:
        column = self._config.uniqueness.columns.review
        self._review_uniqueness = self._df.loc[~self._df[column]].shape[0] / self._N
        return self._review_uniqueness

    def _compute_duplicate_reviews(self) -> int:
        column = self._config.uniqueness.columns.review
        self._duplicate_reviews = self._df.loc[self._df[column]].shape[0]
        return self._duplicate_reviews

    # -------------------------------------------------------------------------------------------- #
    #                                     PRIVACY                                                  #
    # -------------------------------------------------------------------------------------------- #
    def analyze_privacy(self) -> pd.DataFrame:
        df = self.summarize_privacy()
        self.plot_privacy(df=df)
        return df

    def summarize_privacy(self) -> pd.DataFrame:
        columns = self._config.privacy.columns
        df = pd.DataFrame(
            {
                "Anomaly": self._convert_labels(col),
                "Count": self._df.loc[self._df[col]].shape[0],
                "%": self._df.loc[self._df[col]].shape[0] / self._df.shape[0],
            }
            for col in columns
        )
        return df.sort_values(by="%", ascending=False).reset_index(drop=True)

    def _compute_privacy(self) -> float:
        # Retrieve the list of columns to evaluate from the configuration
        columns = self._config.privacy.columns

        # Calculate rows where all specified columns are False
        valid_rows = self._df.loc[~self._df[columns].any(axis=1)]

        # Compute the proportion of valid rows
        self._privacy = valid_rows.shape[0] / self._N

        return self._privacy

    # -------------------------------------------------------------------------------------------- #
    #                                    GET DEFECTS                                               #
    # -------------------------------------------------------------------------------------------- #
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

    # -------------------------------------------------------------------------------------------- #
    #                                       VISUAL                                                 #
    # -------------------------------------------------------------------------------------------- #
    def plot_quality(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_quality()
        viz.barplot(
            data=df,
            x="Dimension",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nQuality Score: {round(self.quality_score,3)}",
        )

    def plot_completeness(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_completeness()
        viz.barplot(
            data=df,
            x="Component",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nCompleteness Score: {round(self.completeness,3)}",
        )

    def plot_validity(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_validity()
        viz.barplot(
            data=df,
            x="Component",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nValidity Score: {round(self.validity,3)}",
        )

    def plot_review_validity(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_review_validity()
        viz.barplot(
            data=df,
            y="Anomaly",
            x="%",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nReview Validity: {round(self.review_validity,3)}",
        )

    def plot_relevance(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_relevance()
        viz.barplot(
            data=df,
            x="Component",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nRelevance Score: {round(self.relevance,3)}",
        )

    def plot_uniqueness(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_uniqueness()
        viz.barplot(
            data=df,
            x="Component",
            y="Score",
            palette="Blues_r",
            title=f"AppVoCAI Dataset Quality Analysis\nUniqueness Score: {round(self.uniqueness,3)}",
        )

    def plot_category_class_balance(
        self, df: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        df = df if df is not None else self._df
        viz.countplot(
            data=df,
            y="category",
            order_by_count=True,
            plot_counts=True,
            title=f"Distribution of Category\nClass Balance Score: {round(self.category_completeness,2)}",
        )

    def plot_privacy(self, df: Optional[pd.DataFrame] = None) -> None:
        df = df if df is not None else self.summarize_privacy()
        viz.barplot(
            data=df,
            y="Anomaly",
            x="%",
            title="Personally Identifiable Information (PII)",
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
        return [col for col in self._df.columns if substring in col]

    def _convert_labels(self, txt) -> str:
        """Converts column names to Title case labels."""
        txt = txt.replace("dqa_", "")
        txt = txt.replace("_", " ")
        txt = txt.title()
        return txt
