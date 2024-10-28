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
# Modified   : Monday October 28th 2024 04:12:56 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import math
import os
import warnings

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"


# ------------------------------------------------------------------------------------------------ #
#                                  COMPUTE TQA SCORE                                               #
# ------------------------------------------------------------------------------------------------ #
class TQATask(Task):
    def __init__(
        self,
        pos_count: float,
        pos_diversity: float,
        pos_intensity: float,
        structural_complexity: float,
        readability: float,
        tqa_check: float,
        tqa_column: str = "tqa_score",
    ) -> None:
        super().__init__()
        self._pos_count = pos_count
        self._pos_diversity = pos_diversity
        self._pos_intensity = pos_intensity
        self._structural_complexity = structural_complexity
        self._readability = readability
        self._tqa_check = tqa_check
        self._tqa_column = tqa_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        @udf(FloatType())
        def compute_tqa_score(row):
            # POS Count Component
            pos_count = (
                row["pos_n_nouns"]
                + row["pos_n_verbs"]
                + row["pos_n_adjectives"]
                + row["pos_n_adverbs"]
            )
            pos_count_score = pos_count * self._pos_count

            # POS Diversity Component (entropy-based calculation)
            pos_tags = [
                row["pos_p_nouns"],
                row["pos_p_verbs"],
                row["pos_p_adjectives"],
                row["pos_p_adverbs"],
            ]
            pos_diversity = -sum(p * math.log(p) for p in pos_tags if p > 0)
            pos_diversity_score = pos_diversity * self._pos_diversity

            # Structural Complexity Component
            structural_complexity = (
                0.4 * row["stats_unique_word_proportion"]
                + 0.3 * row["stats_punctuation_proportion"]
                + 0.3 * row["stats_word_length_std"]
            )
            structural_complexity_score = (
                structural_complexity * self._structural_complexity
            )

            # POS Intensity Component
            pos_intensity = (
                row["pos_n_nouns"]
                + row["pos_n_verbs"]
                + row["pos_n_adjectives"]
                + row["pos_n_adverbs"]
            ) / row["stats_word_count"]
            pos_intensity_score = pos_intensity * self._pos_intensity

            # Readability Component
            readability_score = (
                row["readability_flesch_reading_ease"] * self._readability
            )

            # TQA Check Component
            tqa_check = (
                0.3 * (1 - row["stats_digits_proportion"])
                + 0.3 * (1 - row["stats_punctuation_proportion"])
                + 0.4 * row["tqf_has_terminal_punctuation"]
            )
            tqa_check_score = tqa_check * self._tqa_check

            # Final TQA Score
            score = (
                pos_count_score
                + pos_diversity_score
                + structural_complexity_score
                + pos_intensity_score
                + readability_score
                + tqa_check_score
            )
            return float(score)

        # Apply the UDF conditionally to rows where content length > 2
        data = data.withColumn(
            self._tqa_column,
            F.when(
                F.length(data["content"]) > 2,
                compute_tqa_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )

        return data
