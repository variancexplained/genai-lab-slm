#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/tqa/task.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Monday November 4th 2024 11:12:35 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import math
import os
import warnings

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from explorify.eda.visualize.visualizer import Visualizer
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
sns.set_style("whitegrid")
sns.set_palette("Blues_r")

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=18, verbose=False)


# ------------------------------------------------------------------------------------------------ #
#                                    TQA SCORE 1                                                   #
# ------------------------------------------------------------------------------------------------ #
class TQATask1(Task):
    def __init__(
        self,
        pos_count_weight: float,
        pos_diversity_weight: float,
        pos_intensity_weight: float,
        structural_complexity_weight: float,
        tqa_check_weight: float,
        tqa_column: str = "tqa_score1",
    ) -> None:
        super().__init__()
        self._pos_count_weight = pos_count_weight
        self._pos_diversity_weight = pos_diversity_weight
        self._pos_intensity_weight = pos_intensity_weight
        self._structural_complexity_weight = structural_complexity_weight
        self._tqa_check_weight = tqa_check_weight
        self._tqa_column = tqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:

        @staticmethod
        def compute_pos_count_score(row):
            # POS Count Component
            if len(row.content) > 2:
                pos_count = (
                    row.pos_n_nouns
                    + row.pos_n_verbs
                    + row.pos_n_adjectives
                    + row.pos_n_adverbs
                )
                pos_count_score = pos_count * self._pos_count_weight
            else:
                pos_count_score = 0.0
            return float(pos_count_score)

        def compute_pos_diversity_score(row):
            # POS Diversity Component (entropy-based calculation)
            if len(row.content) > 2:
                pos_tags = [
                    row.pos_p_nouns,
                    row.pos_p_verbs,
                    row.pos_p_adjectives,
                    row.pos_p_adverbs,
                ]
                pos_diversity = -sum(p * math.log(p) for p in pos_tags if p > 0)
                pos_diversity_score = pos_diversity * self._pos_diversity_weight
            else:
                pos_diversity_score = 0.0
            return float(pos_diversity_score)

        def compute_structural_complexity_score(row):
            # Structural Complexity Component
            if len(row.content) > 2:
                structural_complexity = (
                    0.4 * row.stats_unique_word_proportion
                    + 0.3 * row.stats_special_chars_proportion
                    + 0.3 * row.stats_word_length_std
                )
                structural_complexity_score = (
                    structural_complexity * self._structural_complexity_weight
                )
            else:
                structural_complexity_score = 0.0
            return float(structural_complexity_score)

        def compute_pos_intensity_score(row):
            # POS Intensity Component
            if len(row.content) > 2:
                pos_intensity = (
                    row.pos_n_nouns
                    + row.pos_n_verbs
                    + row.pos_n_adjectives
                    + row.pos_n_adverbs
                ) / row.stats_word_count
                pos_intensity_score = pos_intensity * self._pos_intensity_weight
            else:
                pos_intensity_score = 0.0
            return float(pos_intensity_score)

        def compute_tqa_check_score(row):
            # TQA Check Component
            if len(row.content) > 2:
                tqa_check = (
                    0.3 * (1 - row.stats_digits_proportion)
                    + 0.3 * (1 - row.stats_special_chars_proportion)
                    + 0.4 * row.tqf_has_terminal_punctuation
                )
                tqa_check_score = tqa_check * self._tqa_check_weight
            else:
                tqa_check_score = 0.0
            return float(tqa_check_score)

        # Apply the UDF conditionally to rows where content length > 2
        data["tqm_pos_count_score"] = data.parallel_apply(
            compute_pos_count_score, axis=1
        )
        # Calculate min and max values
        min_pos_count = min(data["tqm_pos_count_score"])
        max_pos_count = max(data["tqm_pos_count_score"])

        # Apply min-max normalization to pos_count_score
        data["tqm_pos_count_score"] = (data["tqm_pos_count_score"] - min_pos_count) / (
            max_pos_count - min_pos_count
        )

        # Pos diversity
        data["tqm_pos_diversity_score"] = data.parallel_apply(
            compute_pos_diversity_score, axis=1
        )

        # Structural Complexity
        data["tqm_structural_complexity_score"] = data.parallel_apply(
            compute_structural_complexity_score, axis=1
        )

        # POS Intensity
        data["tqm_pos_intensity_score"] = data.parallel_apply(
            compute_pos_intensity_score, axis=1
        )

        # TQA Quality Check
        data["tqm_tqa_check_score"] = data.parallel_apply(
            compute_tqa_check_score, axis=1
        )

        # Calculate the TQA score as a weighted combination of components
        data["tqa_score1"] = (
            data["tqm_pos_count_score"]
            + data["tqm_pos_diversity_score"]
            + data["tqm_structural_complexity_score"]
            + data["tqm_pos_intensity_score"]
            + data["tqm_tqa_check_score"]
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                    TQA 2 DATA                                                    #
# ------------------------------------------------------------------------------------------------ #
class TQATask2Data(Task):
    def __init__(
        self,
        min_eda_review_length: int = 5,
        max_eda_review_length: int = 1000,
        frac: float = 0.01,
        filepath: str = "data/working/dataprep/tqa_small.csv",
        random_state: int = 202,
    ):
        super().__init__()
        self._min_eda_review_length = min_eda_review_length
        self._max_eda_review_length = max_eda_review_length
        self._frac = frac
        self._filepath = filepath
        self._random_state = random_state

    def run(self, data: pd.DataFrame) -> None:
        df = data.loc[data["eda_review_length"] >= self._min_eda_review_length]
        df = df.loc[df["eda_review_length"] <= self._max_eda_review_length]
        df = df.sample(frac=self._frac, random_state=self._random_state)
        df.to_csv(self._filepath)


# ------------------------------------------------------------------------------------------------ #
#                                    TQA SCORE 2                                                   #
# ------------------------------------------------------------------------------------------------ #
class TQATask2(Task):

    def __init__(
        self,
        ppl_full: float,
        column: str = "content",
        tqa_column: str = "tqa_score2",
        weights_filepath: str = "models/tqa/weights.csv",
    ):
        super().__init__()
        self._ppl_full = ppl_full
        self._column = column
        self._tqa_column = tqa_column
        self._weights_filepath = weights_filepath
        self._weights = self._load_ppl()

        self._weights["Weight"] = self._weights["Perplexity"].apply(
            lambda x: max(0.0, ((self._ppl_full - x) / self._ppl_full))
        )

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        filters = data.columns[data.columns.str.startswith("tqf")].tolist()
        # Obtain binary indicators for each filter
        filter_indicator = data[filters].to_numpy()
        # Compute weighted sum for scores.
        scores = filter_indicator.dot(self._weights["Weight"])
        # Convert numpy array to series
        scores = pd.Series(scores, name=self._tqa_column)
        # Removes the scores column if it already exists
        data = data.drop(columns=["tqa_score2"], errors="ignore")
        # Add scores to DataFrame
        data = pd.concat([data, scores], axis=1)
        return data

    def plot_weights(self, ax: plt.Axes = None) -> plt.Axes:
        """Plots the weights associated with the filters

        Args:
            ax (plt.Axes): A matplotlib Axes object. Optional
        """
        viz = Visualizer()
        df = self._weights.sort_values(by=["Weight"], ascending=False)
        viz.barplot(
            data=df,
            y="Filter",
            x="Weight",
            title="Assigned Weights for Perplexity Filters",
        )

    def _load_ppl(self) -> pd.DataFrame:
        return pd.read_csv(self._weights_filepath)


# ------------------------------------------------------------------------------------------------ #
#                                  TQA SCORE FINAL                                                 #
# ------------------------------------------------------------------------------------------------ #
class TQATask3(Task):
    def __init__(self, tqa1_weight: float = 0.4, tqa2_weight: float = 0.6):
        super().__init__()
        self._tqa1_weight = tqa1_weight
        self._tqa2_weight = tqa2_weight
        self._data = None

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        # Normalize both scores to [0,1]

        # Normalizing scores
        data["tqa_score1"] = (data["tqa_score1"] - data["tqa_score1"].min()) / (
            data["tqa_score1"].max() - data["tqa_score1"].min()
        )
        data["tqa_score2"] = (data["tqa_score2"] - data["tqa_score2"].min()) / (
            data["tqa_score2"].max() - data["tqa_score2"].min()
        )

        # Combine scores using weights.
        data["tqa_score_final"] = (
            self._tqa1_weight * data["tqa_score1"]
            + self._tqa2_weight * data["tqa_score2"]
        )
        self._data = data
        return data

    def plot_scores(self, ax: plt.Axes = None) -> plt.Axes:
        viz = Visualizer()
        viz.kdeplot(
            data=self._data,
            x="tqa_score_final",
            title="Distribution of Text Quality Scores",
        )
