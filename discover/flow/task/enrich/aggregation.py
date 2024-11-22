#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/enrich/aggregation.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 01:57:23 pm                                             #
# Modified   : Thursday November 21st 2024 02:53:00 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ------------------------------------------------------------------------------------------------ #


@dataclass
class AggregateReviewData:
    """
    A dataclass to hold aggregated review data at both the app and category levels.

    Attributes:
        app (DataFrame): Aggregated app-level data.
        category (DataFrame): Aggregated category-level data.
    """

    app: DataFrame
    category: DataFrame


# ------------------------------------------------------------------------------------------------ #
class ReviewAggregation:
    """
    A class to perform app-level and category-level aggregations on a reviews dataset.

    Methods:
        run(data: DataFrame) -> AggregateReviewData:
            Computes app-level and category-level aggregations and returns the results
            as an `AggregateReviewData` dataclass.
    """

    def _app_aggregation(self, data: DataFrame) -> DataFrame:
        """
        Aggregates reviews at the app level.

        Args:
            data (DataFrame): The input PySpark DataFrame containing review data.

        Returns:
            DataFrame: A PySpark DataFrame aggregated at the app level.
        """
        window_spec_vote_sum = Window.partitionBy("app_id").orderBy(F.desc("vote_sum"))
        window_spec_review_length = Window.partitionBy("app_id").orderBy(
            F.desc("en_review_length")
        )

        return data.groupBy("app_id", "category").agg(
            F.count("*").alias("review_count"),  # Total reviews per app
            F.approx_count_distinct("author").alias("author_count"),  # Unique authors
            F.avg("rating").alias("average_rating"),  # Average rating
            F.expr("SUM(CASE WHEN rating = 1 THEN 1 ELSE 0 END) / COUNT(*)").alias(
                "rating_prop_1"
            ),
            F.expr("SUM(CASE WHEN rating = 2 THEN 1 ELSE 0 END) / COUNT(*)").alias(
                "rating_prop_2"
            ),
            F.expr("SUM(CASE WHEN rating = 3 THEN 1 ELSE 0 END) / COUNT(*)").alias(
                "rating_prop_3"
            ),
            F.expr("SUM(CASE WHEN rating = 4 THEN 1 ELSE 0 END) / COUNT(*)").alias(
                "rating_prop_4"
            ),
            F.expr("SUM(CASE WHEN rating = 5 THEN 1 ELSE 0 END) / COUNT(*)").alias(
                "rating_prop_5"
            ),
            F.avg("en_review_length").alias(
                "review_length_avg"
            ),  # Average review length
            F.avg("nrch_review_age").alias("review_age_avg"),  # Average review age
            F.sum("vote_sum").alias("vote_sum_total"),  # Total vote sum
            F.sum("vote_count").alias("vote_count_total"),  # Total vote count
            F.min("date").alias("review_date_first"),  # Earliest review date
            F.avg(F.unix_timestamp("date")).alias(
                "review_date_avg"
            ),  # Average review date
            F.max("date").alias("review_date_last"),  # Latest review date
            F.first(
                F.when(
                    F.col("vote_sum") == F.max("vote_sum").over(window_spec_vote_sum),
                    F.col("content"),
                )
            ).alias("review_highest_vote_sum"),
            F.first(
                F.when(
                    F.col("en_review_length")
                    == F.max("en_review_length").over(window_spec_review_length),
                    F.col("content"),
                )
            ).alias("review_longest"),
        )

    def _category_aggregation(self, app_data: DataFrame) -> DataFrame:
        """
        Aggregates app-level data at the category level.

        Args:
            app_data (DataFrame): The PySpark DataFrame with app-level aggregations.

        Returns:
            DataFrame: A PySpark DataFrame aggregated at the category level.
        """
        # Rank apps within each category
        window_spec_most_reviewed = Window.partitionBy("category").orderBy(
            F.desc("review_count"), F.desc("review_volume")
        )
        app_data = app_data.withColumn(
            "rank_most_reviewed", F.row_number().over(window_spec_most_reviewed)
        )

        # Identify the most reviewed app for each category
        most_reviewed_app = (
            app_data.filter(F.col("rank_most_reviewed") == 1)
            .select("category", "app_name")
            .withColumnRenamed("app_name", "app_most_reviewed")
        )

        # Aggregate at the category level
        category_level_data = app_data.groupBy("category").agg(
            F.sum("review_count").alias(
                "review_count"
            ),  # Total reviews across all apps
            F.sum("review_volume").alias("review_volume"),  # Total review volume
            F.avg("average_rating").alias("average_rating"),  # Average rating
        )

        # Add the most reviewed app to the category-level data
        return category_level_data.join(most_reviewed_app, on="category", how="left")

    def run(self, data: DataFrame) -> AggregateReviewData:
        """
        Executes app-level and category-level aggregations on the reviews dataset.

        Args:
            data (DataFrame): The input PySpark DataFrame containing review data.

        Returns:
            AggregateReviewData: A dataclass containing app-level and category-level
            aggregation results.
        """
        app_data = self._app_aggregation(data)
        category_data = self._category_aggregation(app_data)
        return AggregateReviewData(app=app_data, category=category_data)
