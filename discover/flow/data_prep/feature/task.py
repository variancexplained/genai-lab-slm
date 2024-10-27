#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/feature/task.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Saturday October 26th 2024 10:54:06 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import math
import os
import warnings

import textstat
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sparknlp.annotator import (
    DependencyParserModel,
    PerceptronModel,
    Tokenizer,
    TypedDependencyParserModel,
)
from sparknlp.base import DocumentAssembler, Finisher

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"


# ------------------------------------------------------------------------------------------------ #
class NLPTask(Task):
    """
    A class to perform NLP preprocessing on a specified content column in a Spark DataFrame.
    This task includes tokenization, POS tagging, and formatting of the output as plain lists.

    Attributes
    ----------
    column : str
        The name of the column containing content data to process (default is "content").

    Methods
    -------
    run(data: DataFrame) -> DataFrame
        Executes the NLP pipeline on the provided DataFrame, adding token and POS tag columns.

    _build_pipeline() -> Pipeline
        Constructs a Spark ML Pipeline with stages for document assembly, tokenization,
        POS tagging, and output formatting using a Finisher.
    """

    def __init__(self, column: str = "content") -> None:
        """
        Initializes NLPTask with the column to process.

        Parameters
        ----------
        column : str, optional
            The name of the column containing the content data to process (default is "content").
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the NLP pipeline on the input DataFrame, applying tokenization and POS tagging,
        and returns the transformed DataFrame with additional columns for tokens and POS tags.

        Parameters
        ----------
        data : DataFrame
            The Spark DataFrame containing the content data column specified during initialization.

        Returns
        -------
        DataFrame
            A transformed Spark DataFrame with new columns: 'tokens' and 'pos', containing lists
            of tokens and POS tags, respectively.
        """
        pipeline = self._build_pipeline()
        return pipeline.fit(data).transform(data)

    def _build_pipeline(self) -> Pipeline:
        """
        Builds and returns a Spark ML Pipeline with stages for document assembly, tokenization,
        POS tagging, and a Finisher for output formatting.

        Returns
        -------
        Pipeline
            A configured Spark Pipeline that performs NLP tasks including tokenization, POS tagging,
            and result formatting for easy integration into a DataFrame.
        """
        # Assembles raw content data into a Spark NLP document
        document_assembler = (
            DocumentAssembler().setInputCol(self._column).setOutputCol("document")
        )

        # Tokenizer splits words for NLP processing
        tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("tokens")

        # POS Tagging with a pretrained model
        pos = (
            PerceptronModel.pretrained("pos_ud_ewt", "en")
            .setInputCols(["document", "tokens"])
            .setOutputCol("pos_tags")
        )

        # New stage 1: Dependency Parsing (unlabeled)
        dependency = (
            DependencyParserModel.pretrained("dependency_conllu")
            .setInputCols(["document", "pos_tags", "tokens"])
            .setOutputCol("dependency")
        )

        # New stage 2: Dependency Parsing (labeled)
        dependency_label = (
            TypedDependencyParserModel.pretrained("dependency_typed_conllu")
            .setInputCols(["tokens", "pos_tags", "dependency"])
            .setOutputCol("dependency_type")
        )

        # Finisher converts annotations to plain lists for DataFrame output
        finisher = (
            Finisher()
            .setInputCols(["tokens", "pos_tags", "dependency", "dependency_type"])
            .setOutputCols(
                ["tp_tokens", "tp_pos", "tp_dependency", "tp_dependency_type"]
            )
            .setIncludeMetadata(True)
        )

        # Create and return Pipeline with the defined stages
        pipeline = Pipeline(
            stages=[
                document_assembler,
                tokenizer,
                pos,
                dependency,
                dependency_label,
                finisher,
            ]
        )
        return pipeline


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE COMPLEXITY STATS                                         #
# ------------------------------------------------------------------------------------------------ #
class ComputeComplexityTask(Task):
    def __init__(
        self, column: str = "content", stats_column: str = "stats_complexity"
    ) -> None:
        super().__init__()
        self._column = column
        self._stats_column = stats_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        def compute_review_complexity(parsed_df):
            """
            Compute complexity for each review based on parsed dependencies.

            Complexity is defined as the maximum number of relations (attributes and actions)
            associated with any object (noun) in the review.

            Parameters:
            - parsed_df: DataFrame containing parsed dependencies with columns 'content',
                        'tp_token', 'tp_pos', 'dependency', 'dependency_type'

            Returns:
            - DataFrame with columns 'content' and 'complexity'
            """

            # Step 1: Flatten the DataFrame for easier manipulation
            exploded_df = parsed_df.select(
                F.col(self._column),
                F.explode(
                    F.arrays_zip(
                        F.col("tp_tokens").alias("token"),
                        F.col("tp_pos").alias("pos"),
                        F.col("tp_dependency").alias("dependency"),
                        F.col("tp_dependency_type").alias("dependency_type"),
                    )
                ).alias("cols"),
            ).select(
                self._column,
                F.col("cols.token").alias("token"),
                F.col("cols.pos").alias("pos"),
                F.col("cols.dependency").alias("dependency"),
                F.col("cols.dependency_type").alias("dependency_type"),
            )

            # Step 2: Identify Objects, Attributes, and Actions by POS Tag
            objects_df = exploded_df.filter(
                F.col("pos").isin(["NN", "NNS"])
            )  # Nouns (objects)
            attributes_df = exploded_df.filter(
                F.col("pos").isin(["JJ", "JJR", "JJS"])
            )  # Adjectives (attributes)
            actions_df = exploded_df.filter(
                F.col("pos").isin(["VB", "VBD", "VBG", "VBN", "VBP", "VBZ"])
            )  # Verbs (actions)

            # Step 3: Join on 'dependency' and 'dependency_type' to match relations

            # 1. Relations between objects and attributes
            object_attribute_relations = (
                attributes_df.alias("attributes")
                .join(
                    objects_df.alias("objects"),
                    (F.col("attributes.dependency") == F.col("objects.token"))
                    & (F.col("attributes.dependency_type") == "amod"),
                    "inner",
                )
                .select(
                    F.col("objects.content").alias("content"),
                    F.col("objects.token").alias("object"),
                    F.col("attributes.token").alias("attribute"),
                )
            )

            # 2. Relations between objects and actions
            object_action_relations = (
                actions_df.alias("actions")
                .join(
                    objects_df.alias("objects"),
                    (F.col("actions.dependency") == F.col("objects.token"))
                    & (F.col("actions.dependency_type").isin(["nsubj", "dobj"])),
                    "inner",
                )
                .select(
                    F.col("objects.content").alias("content"),
                    F.col("objects.token").alias("object"),
                    F.col("actions.token").alias("action"),
                )
            )

            # 3. Count Relations for Each Object
            # Combine attribute and action relations, then count relations by object
            object_relations = (
                object_attribute_relations.union(object_action_relations)
                .groupBy(self._column, "object")
                .agg(F.count("*").alias("relation_count"))
            )

            # Step 5: Compute Complexity as the Maximum Number of Relations for Any Object
            complexity_df = object_relations.groupBy(self._column).agg(
                F.max("relation_count").alias(self._stats_column)
            )

            # Drop dependency columns
            complexity_df = complexity_df.drop(
                "dependency", "dependency_type", "object", "relation_count"
            )

            # Merge complexity data into original data frame
            data = parsed_df.join(complexity_df, on=self._column, how="left")

            return data

        return compute_review_complexity(parsed_df=data)


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE POS STATS                                             #
# ------------------------------------------------------------------------------------------------ #
class ComputePOSStatsTask(Task):
    """
    A task to compute Part-of-Speech (POS) statistics for a specified column in a PySpark DataFrame.

    This task generates counts and proportions for specific POS tags (nouns, verbs, adjectives, adverbs, determiners)
    based on POS tags available in the input DataFrame. These statistics are useful for analyzing the linguistic
    characteristics of the text in each row.

    Attributes:
        column (str): The name of the column containing the text or POS data to analyze. Defaults to "content".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the POS statistics calculations on the specified column of the input DataFrame and returns
            the DataFrame with the new POS statistics columns.

    POS Statistics Columns:
        pos_n_nouns (int): The number of noun tags in the text.
        pos_n_verbs (int): The number of verb tags in the text.
        pos_n_adjectives (int): The number of adjective tags in the text.
        pos_n_adverbs (int): The number of adverb tags in the text.
        pos_n_determiners (int): The number of determiner tags in the text.
        pos_p_nouns (float): The proportion of noun tags relative to the total POS tags.
        pos_p_verbs (float): The proportion of verb tags relative to the total POS tags.
        pos_p_adjectives (float): The proportion of adjective tags relative to the total POS tags.
        pos_p_adverbs (float): The proportion of adverb tags relative to the total POS tags.
        pos_p_determiners (float): The proportion of determiner tags relative to the total POS tags.
    """

    def __init__(self, column: str = "content") -> None:
        """
        Initializes the ComputePOSStatsTask with the specified text or POS column.

        Args:
            column (str): The name of the column containing the POS data. Defaults to "content".
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the POS statistics calculations on the specified column.

        The function calculates the counts and proportions of specific POS tags (nouns, verbs, adjectives, adverbs,
        determiners) within each entry of the specified column. The resulting statistics are added as new columns
        in the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the POS tags as a list in the "tp_pos" column.

        Returns:
            DataFrame: The input DataFrame with additional POS statistics columns.
        """

        # Assuming `tp_pos` column contains lists of POS tags for each entry
        # Step 1: Calculate total POS tag count per entry
        data = data.withColumn("pos_count", F.size("tp_pos"))

        # Step 1: Calculate counts of specific POS tags (e.g., NOUN, VERB, ADJ)
        data = data.withColumn(
            "pos_n_nouns", F.expr("size(filter(tp_pos, x -> x = 'NOUN'))")
        )
        data = data.withColumn(
            "pos_n_verbs", F.expr("size(filter(tp_pos, x -> x = 'VERB'))")
        )
        data = data.withColumn(
            "pos_n_adjectives", F.expr("size(filter(tp_pos, x -> x = 'ADJ'))")
        )
        data = data.withColumn(
            "pos_n_adverbs", F.expr("size(filter(tp_pos, x -> x = 'ADV'))")
        )
        data = data.withColumn(
            "pos_n_determiners", F.expr("size(filter(tp_pos, x -> x = 'DET'))")
        )

        # Step 2: Calculate ratios/percentages of specific POS tags
        data = data.withColumn(
            "pos_p_nouns",
            F.when(
                F.col("pos_count") > 0, F.col("pos_n_nouns") / F.col("pos_count")
            ).otherwise(0),
        )
        data = data.withColumn(
            "pos_p_verbs",
            F.when(
                F.col("pos_count") > 0, F.col("pos_n_verbs") / F.col("pos_count")
            ).otherwise(0),
        )
        data = data.withColumn(
            "pos_p_adjectives",
            F.when(
                F.col("pos_count") > 0, F.col("pos_n_adjectives") / F.col("pos_count")
            ).otherwise(0),
        )
        data = data.withColumn(
            "pos_p_adverbs",
            F.when(
                F.col("pos_count") > 0, F.col("pos_n_adverbs") / F.col("pos_count")
            ).otherwise(0),
        )
        data = data.withColumn(
            "pos_p_determiners",
            F.when(
                F.col("pos_count") > 0, F.col("pos_n_determiners") / F.col("pos_count")
            ).otherwise(0),
        )

        # Drop intermediate column if not needed
        data = data.drop("pos_count")

        return data


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE BASIC STATS                                           #
# ------------------------------------------------------------------------------------------------ #
class ComputeBasicStatsTask(Task):
    """
    A task to compute basic text statistics for a specified column in a PySpark DataFrame.

    This task generates various statistics for text data, such as character count, digit and punctuation counts,
    word count, unique word count, and word length statistics, which are useful for analyzing the content and structure
    of text in each row.

    Attributes:
        column (str): The name of the column containing the text data to analyze. Defaults to "content".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the basic statistics calculations on the specified column of the input DataFrame and returns
            the DataFrame with the new statistics columns.

    Basic Statistics Columns:
        stats_char_count (int): The total number of characters in the text.
        stats_digits_count (int): The total number of digits in the text.
        stats_digits_proportion (float): The proportion of digits to total characters.
        stats_punctuation_count (int): The total number of punctuation marks in the text.
        stats_punctuation_proportion (float): The proportion of punctuation marks to total characters.
        stats_word_count (int): The total number of words in the text.
        stats_unique_word_count (int): The total number of unique words in the text.
        stats_unique_word_proportion (float): The proportion of unique words to total words.
        stats_word_length_min (int): The minimum word length in the text.
        stats_word_length_max (int): The maximum word length in the text.
        stats_word_length_mean (float): The mean word length in the text.
        stats_word_length_std (float): The standard deviation of word lengths in the text.
    """

    def __init__(self, column: str = "content") -> None:
        """
        Initializes the ComputeBasicStatsTask with the specified text column.

        Args:
            column (str): The name of the column containing the text data to analyze. Defaults to "content".
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        # 1. Character count
        data = data.withColumn("stats_char_count", F.length(self._column))

        # 2. Digits count
        data = data.withColumn(
            "stats_digits_count",
            F.expr(f"length(regexp_replace({self._column}, '[^0-9]', ''))"),
        )

        # 3. Digits proportion
        data = data.withColumn(
            "stats_digits_proportion",
            F.col("stats_digits_count") / F.col("stats_char_count"),
        )

        # 4. Punctuation count
        data = data.withColumn(
            "stats_punctuation_count",
            F.expr("length(regexp_replace(content, '[^\\p{Punct}]', ''))"),
        )

        # 5. Punctuation proportion
        data = data.withColumn(
            "stats_punctuation_proportion",
            F.col("stats_punctuation_count") / F.col("stats_char_count"),
        )

        # 6. Split content into words
        data = data.withColumn("words", F.split(F.col(self._column), "\\s+"))

        # 7. Word count
        data = data.withColumn("stats_word_count", F.size("words"))

        # 8 Unique word count
        data = data.withColumn("unique_words", F.array_distinct("words"))
        data = data.withColumn("stats_unique_word_count", F.size("unique_words"))

        # 9 Unique word proportion
        data = data.withColumn(
            "stats_unique_word_proportion",
            F.when(
                F.col("stats_word_count") > 0,
                F.col("stats_unique_word_count") / F.col("stats_word_count"),
            ).otherwise(0),
        )

        # Drop intermediate columns
        data = data.drop("words", "unique_words")

        # 10. Word length statistics
        # Split content into words and calculate word lengths
        data = data.withColumn(
            "word_lengths",
            F.expr("transform(split(content, '\\\\s+'), x -> length(x))"),
        )

        # Minimum word length
        data = data.withColumn("stats_word_length_min", F.array_min("word_lengths"))

        # Maximum word length
        data = data.withColumn("stats_word_length_max", F.array_max("word_lengths"))

        # Mean word length
        data = data.withColumn(
            "stats_word_length_mean",
            F.expr(
                "aggregate(transform(word_lengths, x -> CAST(x AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(word_lengths)"
            ),
        )

        # Standard deviation of word length
        data = data.withColumn(
            "stats_word_length_std",
            F.when(
                F.size("word_lengths") > 1,
                F.sqrt(
                    F.expr(
                        "aggregate(transform(word_lengths, x -> CAST(x AS DOUBLE)), CAST(0.0 AS DOUBLE), (acc, x) -> acc + pow(x - stats_word_length_mean, 2)) / size(word_lengths)"
                    )
                ),
            ).otherwise(
                F.lit(None)
            ),  # Use None if you prefer NULL for single-word entries.
        )

        # Drop intermediate column if not needed
        data = data.drop("word_lengths")

        return data


# ------------------------------------------------------------------------------------------------ #
#                                    COMPUTE REVIEW AGE                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewAgeTask(Task):
    """
    A task to compute the "review age" of each entry in a specified date column of a PySpark DataFrame.

    The review age is calculated as the difference in days between the latest date in the column and each
    individual date, providing a measure of how old each review is relative to the most recent one.

    Attributes:
        column (str): The name of the date column to calculate review age from. Defaults to "date".
        stats_column (str): Statistic column to create. Default is 'stats_review_age'.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Calculates the review age for each row in the specified date column and returns the DataFrame
            with the new "stats_review_age" column.

    """

    def __init__(
        self, column: str = "date", stats_column: str = "stats_review_age"
    ) -> None:
        super().__init__()
        self._column = column
        self._stats_column = stats_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the review age calculation on the specified date column.

        The function first identifies the maximum date within the column and then calculates the number of days
        between each review date and this maximum date, storing the result in a new "stats_review_age" column.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the specified date column.

        Returns:
            DataFrame: The input DataFrame with an additional "stats_review_age" column representing the
            review age in days.
        """
        # Step 1: Find the maximum date in the specified column
        max_date = data.agg(F.max(self._column)).first()[0]

        # Step 2: Calculate the "review age" as the difference between max_date and each row's date
        data = data.withColumn(
            self._stats_column, F.datediff(F.lit(max_date), F.col(self._column))
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE AGGREGATE DEVIATION STATS                                  #
# ------------------------------------------------------------------------------------------------ #
class ComputeAggDeviationStats(Task):
    """
    A task to compute the deviation of a specified column's values from the average, grouped by an aggregation key.

    This task calculates the deviation of each value in a specified column from the average of that column within
    groups defined by an aggregation key. This can be used to identify how individual values deviate from the mean
    within a specific context, such as ratings within each app.

    Attributes:
        agg_by (str): The column name to group by for aggregation. Defaults to "app_id".
        column (str): The name of the column for which the deviation is calculated. Defaults to "rating".
        stats_column (str): The name of the output column where the deviation will be stored. Defaults to "stats_deviation_rating".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the deviation calculation for the specified column grouped by the aggregation key, and returns
            the DataFrame with the new deviation column.

    Deviation Calculation Steps:
        1. Calculate the average value of the specified column grouped by the aggregation key.
        2. Join the original DataFrame with the grouped averages on the aggregation key.
        3. Calculate the deviation of each value from the group average and store it in the specified column.
    """

    def __init__(
        self,
        agg_by: str = "app_id",
        column: str = "rating",
        stats_column: str = "stats_deviation_rating",
    ) -> None:
        """Initializes the ComputeAggDeviationStats task with specified aggregation and target columns."""
        super().__init__()
        self._agg_by = agg_by
        self._column = column
        self._stats_column = stats_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        # Step 1: Calculate average for the aggregation
        app_avg_df = data.groupBy(self._agg_by).agg(
            F.avg(self._column).alias(f"stats_{self._column}_avg")
        )
        # Step 2: Join the original DataFrame with the app-level averages
        data = data.join(app_avg_df, on=self._agg_by)

        # Step 3: Calculate deviations
        data = data.withColumn(
            self._stats_column, F.col(self._column) - F.col(f"stats_{self._column}_avg")
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE ENTROPY TASK                                             #
# ------------------------------------------------------------------------------------------------ #
class ComputeEntropyTask(Task):
    """
    A task to compute the entropy of text in a specified column of a PySpark DataFrame.

    Entropy is calculated based on the probability distribution of unique words within each review, providing a measure
    of information density or complexity in the text.

    Attributes:
        column (str): The name of the column containing the text to analyze. Defaults to "content".
        stats_column (str): The name of the column where the computed entropy value will be stored. Defaults to "info_entropy".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the entropy calculation on the specified column of the input DataFrame and returns the
            DataFrame with the new entropy column.

    Entropy Calculation Steps:
        1. Tokenize the text in the specified column into words.
        2. Calculate the frequency of each unique word within the text.
        3. Compute the probability of each unique word based on the total word count.
        4. Apply the entropy formula to the probability distribution to calculate the entropy score.
    """

    def __init__(
        self, column: str = "content", stats_column: str = "info_entropy"
    ) -> None:
        super().__init__()
        self._column = column
        self._stats_column = stats_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        # Step 1: Split content into words
        data = data.withColumn("words", F.split(F.col(self._column), "\\s+"))

        # Step 2: Calculate word counts for each unique word
        data = data.withColumn(
            "word_counts",
            F.expr(
                "transform(array_distinct(words), x -> size(filter(words, w -> w = x)))"
            ),
        )

        # Step 3: Calculate total word count and word probabilities
        data = data.withColumn("total_word_count", F.size("words"))
        data = data.withColumn(
            "word_probabilities",
            F.expr("transform(word_counts, count -> count / total_word_count)"),
        )

        # Step 4: Define a UDF to calculate entropy from word probabilities
        def calculate_entropy(probs):
            if not probs:
                return 0.0
            return -sum(p * math.log2(p) for p in probs if p > 0)

        entropy_udf = F.udf(calculate_entropy, T.DoubleType())

        # Step 5: Apply the UDF to calculate entropy
        data = data.withColumn(
            self._stats_column, entropy_udf(F.col("word_probabilities"))
        )

        # Drop intermediate columns
        data = data.drop(
            "words", "word_counts", "total_word_count", "word_probabilities"
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA STATS TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ComputeTQAStatsTask(Task):
    """
    A task to compute Text Quality Assessment (TQA) statistics for reviews in a PySpark DataFrame.

    This task generates various boolean flags based on the presence of certain parts of speech, punctuation patterns,
    and statistical ratios in the review text. These flags can be used to assess the quality and characteristics
    of each review.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the TQA statistics calculations on the specified columns of the input DataFrame and returns the
            DataFrame with the new TQA columns.

    TQA Columns:
        tqa_has_adjective (bool): True if the review has at least one adjective.
        tqa_has_adverb (bool): True if the review has at least one adverb.
        tqa_has_determiner (bool): True if the review has at least one determiner.
        tqa_has_noun (bool): True if the review has at least one noun.
        tqa_has_terminal_punctuation (bool): True if the review contains terminal punctuation (., !, or ?).
        tqa_has_verb (bool): True if the review has at least one verb.
        tqa_high_digit_ratio (bool): True if the ratio of digits to words is greater than 0.25.
        tqa_high_punctuation_ratio (bool): True if the ratio of punctuation to words is greater than 0.25.
        tqa_word_count_range (bool): True if the word count is between 3 and 256.
    """

    def __init__(self) -> None:
        super().__init__()

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        # 1. Whether review has at least one adjective
        data = data.withColumn("tqa_has_adjective", F.col("pos_n_adjectives") > 0)

        # 2. Whether review has at least one adverb
        data = data.withColumn("tqa_has_adverb", F.col("pos_n_adverbs") > 0)

        # 3. Whether review has at least one determiner
        data = data.withColumn("tqa_has_determiner", F.col("pos_n_determiners") > 0)

        # 4. Whether review has at least one noun
        data = data.withColumn("tqa_has_noun", F.col("pos_n_nouns") > 0)

        # 5. Whether the review contains terminal punctuation (., !, or ?)
        data = data.withColumn(
            "tqa_has_terminal_punctuation", F.col("content").rlike("[.!?]$")
        )

        # 6. Whether review has at least one verb
        data = data.withColumn("tqa_has_verb", F.col("pos_n_verbs") > 0)

        # 7. Whether digits to words ratio is greater than 0.25
        data = data.withColumn(
            "tqa_high_digit_ratio", F.col("stats_digits_proportion") > 0.25
        )

        # 8. Whether punctuation to words ratio is greater than 0.25
        data = data.withColumn(
            "tqa_high_punctuation_ratio", F.col("stats_punctuation_proportion") > 0.25
        )

        # 9. Whether word count is in the range > 3 and < 256
        data = data.withColumn(
            "tqa_word_count_range",
            (F.col("stats_word_count") > 3) & (F.col("stats_word_count") < 256),
        )

        # 10. Whether complexity is at least 1.
        data = data.withColumn("tqa_complexity_c1", (F.col("stats_complexity") > 0))

        # 11. Whether complexity is at least 2.
        data = data.withColumn("tqa_complexity_c1", (F.col("stats_complexity") > 1))

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE READABILITY TASK                                         #
# ------------------------------------------------------------------------------------------------ #
class ComputeReadabilityTask(Task):
    """
    A task to compute the Flesch Reading Ease score for a specified column in a PySpark DataFrame.

    This task applies the Flesch Reading Ease formula to evaluate the readability of text in each row of the specified
    column. The result is stored in a new column within the DataFrame.

    Attributes:
        column (str): The name of the column containing the text to analyze. Defaults to "content".
        stats_column (str): The name of the column where the readability score will be stored. Defaults to
            "readability_flesch_reading_ease".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the readability calculation on the specified column of the input DataFrame and returns the
            DataFrame with the new readability score column.
    """

    def __init__(
        self,
        column: str = "content",
        stats_column: str = "readability_flesch_reading_ease",
    ) -> None:
        super().__init__()
        self._column = column
        self._stats_column = stats_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        def calculate_flesch_reading_ease(text):
            if text:
                return textstat.flesch_reading_ease(text)
            return None  # Return None if text is empty or null

        # Register the function as a PySpark UDF
        flesch_reading_ease_udf = F.udf(calculate_flesch_reading_ease, T.DoubleType())

        # Apply the UDF to the specified column to create the readability score column
        data = data.withColumn(
            self._stats_column, flesch_reading_ease_udf(F.col(self._column))
        )
        return data
