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
# Modified   : Tuesday October 29th 2024 09:17:37 pm                                               #
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
from pyspark.sql.functions import col, udf
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
        tqa_check: float,
        tqa_column: str = "tqa_score",
    ) -> None:
        super().__init__()
        self._pos_count = pos_count
        self._pos_diversity = pos_diversity
        self._pos_intensity = pos_intensity
        self._structural_complexity = structural_complexity
        self._tqa_check = tqa_check
        self._tqa_column = tqa_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        @udf(FloatType())
        def compute_pos_count_score(row):
            # POS Count Component
            pos_count = (
                row["pos_n_nouns"]
                + row["pos_n_verbs"]
                + row["pos_n_adjectives"]
                + row["pos_n_adverbs"]
            )
            pos_count_score = pos_count * self._pos_count
            return float(pos_count_score)

        @udf(FloatType())
        def compute_pos_diversity_score(row):
            # POS Diversity Component (entropy-based calculation)
            pos_tags = [
                row["pos_p_nouns"],
                row["pos_p_verbs"],
                row["pos_p_adjectives"],
                row["pos_p_adverbs"],
            ]
            pos_diversity = -sum(p * math.log(p) for p in pos_tags if p > 0)
            pos_diversity_score = pos_diversity * self._pos_diversity
            return float(pos_diversity_score)

        @udf(FloatType())
        def compute_structural_complexity_score(row):
            # Structural Complexity Component
            structural_complexity = (
                0.4 * row["stats_unique_word_proportion"]
                + 0.3 * row["stats_punctuation_proportion"]
                + 0.3 * row["stats_word_length_std"]
            )
            structural_complexity_score = (
                structural_complexity * self._structural_complexity
            )
            return float(structural_complexity_score)

        @udf(FloatType())
        def compute_pos_intensity_score(row):
            # POS Intensity Component
            pos_intensity = (
                row["pos_n_nouns"]
                + row["pos_n_verbs"]
                + row["pos_n_adjectives"]
                + row["pos_n_adverbs"]
            ) / row["stats_word_count"]
            pos_intensity_score = pos_intensity * self._pos_intensity
            return float(pos_intensity_score)

        @udf(FloatType())
        def compute_tqa_check_score(row):
            # TQA Check Component
            tqa_check = (
                0.3 * (1 - row["stats_digits_proportion"])
                + 0.3 * (1 - row["stats_punctuation_proportion"])
                + 0.4 * row["tqf_has_terminal_punctuation"]
            )
            tqa_check_score = tqa_check * self._tqa_check
            return float(tqa_check_score)

        # Apply the UDF conditionally to rows where content length > 2
        data = data.withColumn(
            "tqm_pos_count_score",
            F.when(
                F.length(data["content"]) > 2,
                compute_pos_count_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )
        # Calculate min and max values
        min_pos_count = data.agg(F.min("tqm_pos_count_score")).collect()[0][0]
        max_pos_count = data.agg(F.max("tqm_pos_count_score")).collect()[0][0]

        # Apply min-max normalization to pos_count_score
        data = data.withColumn(
            "tqm_pos_count_score",
            (col("tqm_pos_count_score") - min_pos_count)
            / (max_pos_count - min_pos_count),
        )

        data = data.withColumn(
            "tqm_pos_diversity_score",
            F.when(
                F.length(data["content"]) > 2,
                compute_pos_diversity_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )
        data = data.withColumn(
            "tqm_structural_complexity_score",
            F.when(
                F.length(data["content"]) > 2,
                compute_structural_complexity_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )
        data = data.withColumn(
            "tqm_pos_intensity_score",
            F.when(
                F.length(data["content"]) > 2,
                compute_pos_intensity_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )
        data = data.withColumn(
            "tqm_tqa_check_score",
            F.when(
                F.length(data["content"]) > 2,
                compute_tqa_check_score(F.struct(*data.columns)),
            ).otherwise(0.0),
        )
        # Calculate the TQA score as a weighted combination of components
        data = data.withColumn(
            "tqa_score",
            col("tqm_pos_count_score")
            + col("tqm_pos_diversity_score")
            + col("tqm_structural_complexity_score")
            + col("tqm_pos_intensity_score")
            + col("tqm_tqa_check_score"),
        )
        return data
