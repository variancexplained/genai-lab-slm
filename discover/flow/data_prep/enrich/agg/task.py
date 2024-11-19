#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/agg/task.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 8th 2024 12:06:29 am                                                #
# Modified   : Monday November 18th 2024 03:58:25 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
class AppAggregationTask(Task):
    """
    A task that aggregates review data at the app level, computing various summary statistics
    and metrics such as review counts, average ratings, and highest scores.

    This class ranks reviews based on length, vote sum, and TQA score, then aggregates
    the data to produce a comprehensive overview of reviews for each app, including
    identifying reviews with the highest vote sum, the highest TQA score, and the longest review.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        dataset_name (str): Name of the dataset created

    Methods:
        run(data: DataFrame) -> DataFrame:
            Aggregates the input DataFrame at the app level and returns the aggregated results.
    """

    def __init__(
        self,
        dataset_name: str,
    ) -> None:
        super().__init__()
        self._dataset_name = dataset_name

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the aggregation at the app level, adding ranking columns to identify
        specific reviews and calculating various summary statistics.

        Args:
            data (DataFrame): The input PySpark DataFrame with columns:
                app_id, app_name, category_id, category, author, rating, content, vote_sum,
                vote_count, date, nrch_review_length, nrch_review_age,
                enrichment_tqa_score1, enrichment_tqa_score2, enrichment_tqa_score_final.

        Returns:
            DataFrame: A PySpark DataFrame aggregated at the app level with the following columns:
                - app_id, app_name, category_id, category
                - review_count: Total number of reviews for each app.
                - author_count: Number of unique authors for each app.
                - average_rating: Average rating for each app.
                - average_review_length: Average length of reviews for each app.
                - average_review_age: Average age of reviews for each app.
                - total_vote_sum: Total sum of votes for each app.
                - total_vote_count: Total number of votes for each app.
                - first_review_date: Earliest review date for each app.
                - avg_review_date: Average review date in Unix timestamp format.
                - last_review_date: Latest review date for each app.
                - average_tqa_score_1: Average TQA score 1 for each app.
                - average_tqa_score_2: Average TQA score 2 for each app.
                - average_tqa_score_final: Average final TQA score for each app.
                - max_tqa_score_1: Maximum TQA score 1 for each app.
                - max_tqa_score_2: Maximum TQA score 2 for each app.
                - max_tqa_score_final: Maximum final TQA score for each app.
                - review_highest_vote_sum: Content of the review with the highest vote sum for each app.
                - review_highest_tqa_score_final: Content of the review with the highest TQA score for each app.
                - review_longest: Content of the longest review for each app.
        """
        # Define a window specification to rank reviews within each app
        window_spec_vote_sum = Window.partitionBy("app_id").orderBy(F.desc("vote_sum"))
        window_spec_tqa_score = Window.partitionBy("app_id").orderBy(
            F.desc("tqa_score")
        )
        window_spec_review_length = Window.partitionBy("app_id").orderBy(
            F.desc("review_length")
        )

        # Add columns for the review length, highest vote sum, and highest TQA score review content
        data = data.withColumn(
            "rank_review_length", F.rank().over(window_spec_review_length)
        )
        data = data.withColumn("rank_vote_sum", F.rank().over(window_spec_vote_sum))
        data = data.withColumn("rank_tqa_score", F.rank().over(window_spec_tqa_score))

        # Aggregate at the app level
        aggregated_data = data.groupBy(
            "app_id", "app_name", "category_id", "category"
        ).agg(
            F.count("*").alias("review_count"),
            F.approx_count_distinct("author").alias("author_count"),
            F.avg("rating").alias("average_rating"),
            F.avg("review_length").alias("average_review_length"),
            F.avg("nrch_review_age").alias("average_review_age"),
            F.sum("vote_sum").alias("total_vote_sum"),
            F.sum("vote_count").alias("total_vote_count"),
            F.min("date").alias("first_review_date"),
            F.avg(F.unix_timestamp("date")).alias("avg_review_date"),
            F.max("date").alias("last_review_date"),
            F.avg("voc_sentiment_score").alias("average_sentiment"),
            F.avg("tqa_syntactic_lexical_score").alias("average_tqa_score_1"),
            F.avg("tqa_perplexity_score").alias("average_tqa_score_2"),
            F.avg("tqa_score").alias("average_tqa_score_final"),
            F.max("tqa_syntactic_lexical_score").alias("max_tqa_score_1"),
            F.max("tqa_perplexity_score").alias("max_tqa_score_2"),
            F.max("tqa_score").alias("max_tqa_score_final"),
            F.first(F.when(F.col("rank_vote_sum") == 1, F.col("content"))).alias(
                "review_highest_vote_sum"
            ),
            F.first(F.when(F.col("rank_tqa_score") == 1, F.col("content"))).alias(
                "review_highest_tqa_score_final"
            ),
            F.first(F.when(F.col("rank_review_length") == 1, F.col("content"))).alias(
                "review_longest"
            ),
        )

        # Drop the temporary rank columns
        aggregated_data = aggregated_data.drop(
            "rank_vote_sum", "rank_tqa_score", "rank_review_length"
        )

        return aggregated_data


# ------------------------------------------------------------------------------------------------ #
class CategoryAggregationTask(Task):
    """
    A task that aggregates review data at the category level, computing various summary statistics
    and metrics such as review counts, average ratings, and highest scores.

    This class ranks reviews based on length, vote sum, and TQA score, then aggregates
    the data to produce a comprehensive overview of reviews for each category, including
    identifying reviews with the highest vote sum, the highest TQA score, and the longest review.

    Args:
        stage_id (str): Id for the stage to which the task belongs.
        dataset_name (str): Name of the dataset created

    Methods:
        run(data: DataFrame) -> DataFrame:
            Aggregates the input DataFrame at the category level and returns the aggregated results.
    """

    def __init__(
        self,
        dataset_name: str,
    ) -> None:
        """
        Initializes the AppAggregationTask.
        """
        super().__init__()
        self._dataset_name = dataset_name

    @property
    def dataset_name(self) -> str:
        return self._dataset_name

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the aggregation at the category level, adding ranking columns to identify
        specific reviews and calculating various summary statistics.

        Args:
            data (DataFrame): The input PySpark DataFrame with columns:
                category_id, category, author, rating, content, vote_sum, vote_count, date,
                nrch_review_length, nrch_review_age, enrichment_tqa_score1,
                enrichment_tqa_score2, enrichment_tqa_score_final.

        Returns:
            DataFrame: A PySpark DataFrame aggregated at the category level with the following columns:
                - category_id, category
                - review_count: Total number of reviews for each category.
                - author_count: Number of unique authors for each category.
                - average_rating: Average rating for each category.
                - average_review_length: Average length of reviews for each category.
                - average_review_age: Average age of reviews for each category.
                - total_vote_sum: Total sum of votes for each category.
                - total_vote_count: Total number of votes for each category.
                - first_review_date: Earliest review date for each category.
                - avg_review_date: Average review date in Unix timestamp format.
                - last_review_date: Latest review date for each category.
                - average_tqa_score_1: Average TQA score 1 for each category.
                - average_tqa_score_2: Average TQA score 2 for each category.
                - average_tqa_score_final: Average final TQA score for each category.
                - max_tqa_score_1: Maximum TQA score 1 for each category.
                - max_tqa_score_2: Maximum TQA score 2 for each category.
                - max_tqa_score_final: Maximum final TQA score for each category.
                - review_highest_vote_sum: Content of the review with the highest vote sum for each category.
                - review_highest_tqa_score_final: Content of the review with the highest TQA score for each category.
                - review_longest: Content of the longest review for each category.
        """
        # Define a window specification to rank reviews within each category
        window_spec_vote_sum = Window.partitionBy("category_id").orderBy(
            F.desc("vote_sum")
        )
        window_spec_tqa_score = Window.partitionBy("category_id").orderBy(
            F.desc("tqa_score")
        )
        window_spec_review_length = Window.partitionBy("category_id").orderBy(
            F.desc("review_length")
        )

        # Add columns for the review length, highest vote sum, and highest TQA score review content
        data = data.withColumn(
            "rank_review_length", F.rank().over(window_spec_review_length)
        )
        data = data.withColumn("rank_vote_sum", F.rank().over(window_spec_vote_sum))
        data = data.withColumn("rank_tqa_score", F.rank().over(window_spec_tqa_score))

        # Aggregate at the category level
        aggregated_data = data.groupBy("category_id", "category").agg(
            F.count("*").alias("review_count"),
            F.approx_count_distinct("author").alias("author_count"),
            F.avg("rating").alias("average_rating"),
            F.avg("review_length").alias("average_review_length"),
            F.avg("nrch_review_age").alias("average_review_age"),
            F.sum("vote_sum").alias("total_vote_sum"),
            F.sum("vote_count").alias("total_vote_count"),
            F.min("date").alias("first_review_date"),
            F.avg(F.unix_timestamp("date")).alias("avg_review_date"),
            F.max("date").alias("last_review_date"),
            F.avg("voc_sentiment_score").alias("average_sentiment"),
            F.avg("tqa_syntactic_lexical_score").alias("average_tqa_score_1"),
            F.avg("tqa_perplexity_score").alias("average_tqa_score_2"),
            F.avg("tqa_score").alias("average_tqa_score_final"),
            F.max("tqa_syntactic_lexical_score").alias("max_tqa_score_1"),
            F.max("tqa_perplexity_score").alias("max_tqa_score_2"),
            F.max("tqa_score").alias("max_tqa_score_final"),
            F.first(F.when(F.col("rank_vote_sum") == 1, F.col("content"))).alias(
                "review_highest_vote_sum"
            ),
            F.first(F.when(F.col("rank_tqa_score") == 1, F.col("content"))).alias(
                "review_highest_tqa_score_final"
            ),
            F.first(F.when(F.col("rank_review_length") == 1, F.col("content"))).alias(
                "review_longest"
            ),
        )

        # Drop the temporary rank columns
        aggregated_data = aggregated_data.drop(
            "rank_vote_sum", "rank_tqa_score", "rank_review_length"
        )

        return aggregated_data
