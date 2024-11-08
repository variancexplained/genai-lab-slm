#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/quality/task.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:03:10 pm                                              #
# Modified   : Friday November 8th 2024 04:03:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Module"""
import math

import pyspark.pandas as ps
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sparknlp.annotator import PerceptronModel, Tokenizer
from sparknlp.base import DocumentAssembler, Finisher

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
#                                       NLP TASK                                                   #
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

        # Finisher converts annotations to plain lists for DataFrame output
        finisher = (
            Finisher()
            .setInputCols(["tokens", "pos_tags"])
            .setOutputCols(["tp_tokens", "tp_pos"])
        )

        # Create and return Pipeline with the defined stages
        pipeline = Pipeline(
            stages=[
                document_assembler,
                tokenizer,
                pos,
                finisher,
            ]
        )
        return pipeline


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
        stats_special_chars_count (int): The total number of punctuation marks in the text.
        stats_special_chars_proportion (float): The proportion of punctuation marks to total characters.
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
            F.expr("regexp_count(content, '[^0-9]')"),
        )

        # 3. Digits proportion
        data = data.withColumn(
            "stats_digits_proportion",
            F.when(
                F.col("stats_char_count") > 0,
                F.col("stats_digits_count") / F.col("stats_char_count"),
            ).otherwise(0),
        )

        # 4. Special chars count
        data = data.withColumn(
            "stats_special_chars_count",
            F.expr("regexp_count(content, r'[^\\w\\s]')"),
        )

        # 5. Special chars proportion
        data = data.withColumn(
            "stats_special_chars_proportion",
            F.when(
                F.col("stats_char_count") > 0,
                F.col("stats_special_chars_count") / F.col("stats_char_count"),
            ).otherwise(0),
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

        # 10 Word Repetition Ratio
        data = data.withColumn(
            "stats_word_repetition_ratio", 1 - F.col("stats_unique_word_proportion")
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
            ).otherwise(0),
        )

        # Drop intermediate column if not needed
        data = data.drop("word_lengths")

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA STATS TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ComputeTQAFiltersTask(Task):
    """
    A task to compute Text Quality Assessment (TQA) statistics for reviews in a PySpark DataFrame.

    This task generates various boolean flags based on the presence of certain parts of speech, punctuation patterns,
    and statistical ratios in the review text. These flags can be used to assess the quality and characteristics
    of each review.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the TQA statistics calculations on the specified columns of the input DataFrame and returns the
            DataFrame with the new TQA columns.

    TQA Filter Columns:
        tqf_has_adjective (bool): True if the review has at least one adjective.
        tqf_has_adverb (bool): True if the review has at least one adverb.
        tqf_has_determiner (bool): True if the review has at least one determiner.
        tqf_has_noun (bool): True if the review has at least one noun.
        tqf_has_terminal_punctuation (bool): True if the review contains terminal punctuation (., !, or ?).
        tqf_has_verb (bool): True if the review has at least one verb.
        tqf_high_digit_ratio (bool): True if the ratio of digits to words is greater than 0.25.
        tqf_high_punctuation_ratio (bool): True if the ratio of punctuation to words is greater than 0.25.
        tqf_word_count_range (bool): True if the word count is between 3 and 256.
    """

    def __init__(self) -> None:
        super().__init__()

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:

        # 1. Whether review has at least one adjective
        data = data.withColumn("tqf_has_adjective", F.col("pos_n_adjectives") > 0)

        # 2. Whether review has at least one adverb
        data = data.withColumn("tqf_has_adverb", F.col("pos_n_adverbs") > 0)

        # 3. Whether review has at least one determiner
        data = data.withColumn("tqf_has_determiner", F.col("pos_n_determiners") > 0)

        # 4. Whether review has at least one noun
        data = data.withColumn("tqf_has_noun", F.col("pos_n_nouns") > 0)

        # 5. Whether the review contains terminal punctuation (., !, or ?)
        data = data.withColumn(
            "tqf_has_terminal_punctuation", F.col("content").rlike("[.!?]$")
        )

        # 6. Whether review has at least one verb
        data = data.withColumn("tqf_has_verb", F.col("pos_n_verbs") > 0)

        # 7. Whether punctuation to words ratio is greater than 0.25
        data = data.withColumn(
            "tqf_high_special_chars_ratio",
            F.col("stats_special_chars_proportion") > 0.25,
        )

        # 8. Whether word count is in the range > 3 and < 256
        data = data.withColumn(
            "tqf_word_count_range",
            (F.col("stats_word_count") > 3) & (F.col("stats_word_count") < 256),
        )

        # 9. Stop wprd match
        # List of stop words to search for
        stop_words = ["the", "be", "to", "of", "and", "that", "have", "with"]

        # Create conditions for each stop word
        conditions = [
            F.expr(f"array_contains(split(content, ' '), '{word}')").cast("int")
            for word in stop_words
        ]

        # Sum the conditions and check if at least 2 stop words are present
        data = data.withColumn(
            "tqf_stop_word_match", F.when(sum(conditions) >= 2, True).otherwise(False)
        )

        # 10. Create a new column "tqf_first_letter_cap" based on the first letter being uppercase
        data = data.withColumn(
            "tqf_first_letter_cap", F.expr("substring(content, 1, 1) rlike '^[A-Z]'")
        )

        # 11. Create a new column "tqf_no_all_caps" based on whether the content is all caps
        data = data.withColumn("tqf_no_all_caps", ~F.col("content").rlike("^[^a-z]*$"))

        # 12. Create a new column "tqf_high_word_repetition" if 'stats_word_repetition_ratio' >= 0.2
        data = data.withColumn(
            "tqf_high_word_repetition", F.col("stats_word_repetition_ratio") >= 0.2
        )

        # Define the regex pattern for special characters (non-alphanumeric, non-punctuation)
        special_chars_pattern = r"[^a-zA-Z0-9\s.,!?;:'\"()\-]"

        # Set tqf_no_special_chars to True if content has no special characters
        data = data.withColumn(
            "tqf_no_special_chars", ~F.col("content").rlike(special_chars_pattern)
        )

        # Delete tokens an pos tags from dataset
        data = data.drop("tp_tokens", "tp_pos")

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE 1 TASK                                         #
# ------------------------------------------------------------------------------------------------ #
class TQATask1(Task):
    """
    A task to compute a Text Quality Assessment (TQA) score based on various components
    such as POS count, POS diversity, structural complexity, POS intensity, and TQA quality checks.

    Attributes:
        pos_count_weight (float): The weight assigned to the POS count component.
        pos_diversity_weight (float): The weight assigned to the POS diversity component.
        pos_intensity_weight (float): The weight assigned to the POS intensity component.
        structural_complexity_weight (float): The weight assigned to the structural complexity component.
        tqa_check_weight (float): The weight assigned to the TQA quality check component.
        column (str): Column containing review text.
        new_column (str): The name of the output column to store the computed TQA score.
    """

    def __init__(
        self,
        pos_count_weight: float,
        pos_diversity_weight: float,
        pos_intensity_weight: float,
        structural_complexity_weight: float,
        tqa_check_weight: float,
        column: str = "content",
        new_column: str = "enrichment_tqa_score1",
    ) -> None:
        """
        Initializes the TQATask1 with specified weights and output column name.

        Args:
            pos_count_weight (float): Weight for the POS count component.
            pos_diversity_weight (float): Weight for the POS diversity component.
            pos_intensity_weight (float): Weight for the POS intensity component.
            structural_complexity_weight (float): Weight for the structural complexity component.
            tqa_check_weight (float): Weight for the TQA quality check component.
            new_column (str): Name of the output column for the TQA score. Defaults to "enrichment_tqa_score1".
        """
        super().__init__()
        self._pos_count_weight = pos_count_weight
        self._pos_diversity_weight = pos_diversity_weight
        self._pos_intensity_weight = pos_intensity_weight
        self._structural_complexity_weight = structural_complexity_weight
        self._tqa_check_weight = tqa_check_weight
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the TQA score computation by applying several components as UDFs.

        Args:
            data (DataFrame): The input PySpark DataFrame containing text data and related features.

        Returns:
            DataFrame: The input DataFrame with additional columns for each component score
            and the final TQA score.
        """

        # Define UDFs for each computation
        @F.udf("float")
        def compute_pos_count_score(
            content, pos_n_nouns, pos_n_verbs, pos_n_adjectives, pos_n_adverbs
        ):
            """
            Computes the POS count score based on the provided content and POS counts.

            Args:
                content (str): The text content of the review.
                pos_n_nouns (int): Number of nouns in the content.
                pos_n_verbs (int): Number of verbs in the content.
                pos_n_adjectives (int): Number of adjectives in the content.
                pos_n_adverbs (int): Number of adverbs in the content.

            Returns:
                float: The computed POS count score.
            """
            if len(content) > 2:
                pos_count = pos_n_nouns + pos_n_verbs + pos_n_adjectives + pos_n_adverbs
                return float(pos_count * self._pos_count_weight)
            return 0.0

        @F.udf("float")
        def compute_pos_diversity_score(
            content, pos_p_nouns, pos_p_verbs, pos_p_adjectives, pos_p_adverbs
        ):
            """
            Computes the POS diversity score using an entropy-based calculation.

            Args:
                content (str): The text content of the review.
                pos_p_nouns (float): Proportion of nouns in the content.
                pos_p_verbs (float): Proportion of verbs in the content.
                pos_p_adjectives (float): Proportion of adjectives in the content.
                pos_p_adverbs (float): Proportion of adverbs in the content.

            Returns:
                float: The computed POS diversity score.
            """
            if len(content) > 2:
                pos_tags = [pos_p_nouns, pos_p_verbs, pos_p_adjectives, pos_p_adverbs]
                pos_diversity = -sum(p * math.log(p) for p in pos_tags if p > 0)
                return float(pos_diversity * self._pos_diversity_weight)
            return 0.0

        @F.udf("float")
        def compute_structural_complexity_score(
            content,
            stats_unique_word_proportion,
            stats_special_chars_proportion,
            stats_word_length_std,
        ):
            """
            Computes the structural complexity score based on unique word proportion,
            special character proportion, and word length standard deviation.

            Args:
                content (str): The text content of the review.
                stats_unique_word_proportion (float): Proportion of unique words in the content.
                stats_special_chars_proportion (float): Proportion of special characters in the content.
                stats_word_length_std (float): Standard deviation of word lengths in the content.

            Returns:
                float: The computed structural complexity score.
            """
            if len(content) > 2:
                structural_complexity = (
                    0.4 * stats_unique_word_proportion
                    + 0.3 * stats_special_chars_proportion
                    + 0.3 * stats_word_length_std
                )
                return float(structural_complexity * self._structural_complexity_weight)
            return 0.0

        @F.udf("float")
        def compute_pos_intensity_score(
            content,
            pos_n_nouns,
            pos_n_verbs,
            pos_n_adjectives,
            pos_n_adverbs,
            stats_word_count,
        ):
            """
            Computes the POS intensity score based on the number of POS tags relative
            to the word count.

            Args:
                content (str): The text content of the review.
                pos_n_nouns (int): Number of nouns in the content.
                pos_n_verbs (int): Number of verbs in the content.
                pos_n_adjectives (int): Number of adjectives in the content.
                pos_n_adverbs (int): Number of adverbs in the content.
                stats_word_count (int): Total word count in the content.

            Returns:
                float: The computed POS intensity score.
            """
            if len(content) > 2 and stats_word_count > 0:
                pos_intensity = (
                    pos_n_nouns + pos_n_verbs + pos_n_adjectives + pos_n_adverbs
                ) / stats_word_count
                return float(pos_intensity * self._pos_intensity_weight)
            return 0.0

        @F.udf("float")
        def compute_tqa_check_score(
            content,
            stats_digits_proportion,
            stats_special_chars_proportion,
            tqf_has_terminal_punctuation,
        ):
            """
            Computes the TQA check score based on digit and special character proportions
            and the presence of terminal punctuation.

            Args:
                content (str): The text content of the review.
                stats_digits_proportion (float): Proportion of digits in the content.
                stats_special_chars_proportion (float): Proportion of special characters in the content.
                tqf_has_terminal_punctuation (float): Indicator of whether the content has terminal punctuation.

            Returns:
                float: The computed TQA check score.
            """
            if len(content) > 2:
                tqa_check = (
                    0.3 * (1 - stats_digits_proportion)
                    + 0.3 * (1 - stats_special_chars_proportion)
                    + 0.4 * tqf_has_terminal_punctuation
                )
                return float(tqa_check * self._tqa_check_weight)
            return 0.0

        # Apply UDFs to create new columns
        data = data.withColumn(
            "tqm_pos_count_score",
            compute_pos_count_score(
                F.col(self._column),
                F.col("pos_n_nouns"),
                F.col("pos_n_verbs"),
                F.col("pos_n_adjectives"),
                F.col("pos_n_adverbs"),
            ),
        )

        # Min-max normalization for pos_count_score
        min_pos_count, max_pos_count = data.select(
            F.min("tqm_pos_count_score"), F.max("tqm_pos_count_score")
        ).first()
        data = data.withColumn(
            "tqm_pos_count_score",
            (F.col("tqm_pos_count_score") - min_pos_count)
            / (max_pos_count - min_pos_count),
        )

        # Apply other UDFs
        data = data.withColumn(
            "tqm_pos_diversity_score",
            compute_pos_diversity_score(
                F.col(self._column),
                F.col("pos_p_nouns"),
                F.col("pos_p_verbs"),
                F.col("pos_p_adjectives"),
                F.col("pos_p_adverbs"),
            ),
        )

        data = data.withColumn(
            "tqm_structural_complexity_score",
            compute_structural_complexity_score(
                F.col(self._column),
                F.col("stats_unique_word_proportion"),
                F.col("stats_special_chars_proportion"),
                F.col("stats_word_length_std"),
            ),
        )

        data = data.withColumn(
            "tqm_pos_intensity_score",
            compute_pos_intensity_score(
                F.col(self._column),
                F.col("pos_n_nouns"),
                F.col("pos_n_verbs"),
                F.col("pos_n_adjectives"),
                F.col("pos_n_adverbs"),
                F.col("stats_word_count"),
            ),
        )

        data = data.withColumn(
            "tqm_tqa_check_score",
            compute_tqa_check_score(
                F.col(self._column),
                F.col("stats_digits_proportion"),
                F.col("stats_special_chars_proportion"),
                F.col("tqf_has_terminal_punctuation"),
            ),
        )

        # Calculate the TQA score as a weighted combination of components
        data = data.withColumn(
            self._new_column,
            F.col("tqm_pos_count_score")
            + F.col("tqm_pos_diversity_score")
            + F.col("tqm_structural_complexity_score")
            + F.col("tqm_pos_intensity_score")
            + F.col("tqm_tqa_check_score"),
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE 2 TASK                                         #
# ------------------------------------------------------------------------------------------------ #
class TQATask2(Task):
    """
    A task to compute a second Text Quality Assessment (TQA) score based on a weighted sum of binary indicators
    and weights derived from perplexity values.

    Attributes:
        ppl_full (float): The perplexity value for normalization.
        column (str): The name of the column in the DataFrame containing text data. Defaults to "content".
        new_column (str): The name of the output column for the computed TQA score. Defaults to "enrichment_tqa_score2".
        ppl_filepath (str): Path to file containing filtered perplexity scores. Defaults to "models/tqa/tqa_ppl.csv".
    """

    def __init__(
        self,
        ppl_full: float,
        column: str = "content",
        new_column: str = "enrichment_tqa_score2",
        ppl_filepath: str = "models/tqa/tqa_ppl.csv",
    ):
        """
        Initializes the TQATask2 with specified parameters and loads weights for computation.

        Args:
            ppl_full (float): The perplexity value for normalization.
            column (str): The name of the column in the DataFrame containing text data. Defaults to "content".
            new_column (str): The name of the output column for the computed TQA score. Defaults to "enrichment_tqa_score2".
            ppl_filepath (str): Path to file containing filtered perplexity scores. Defaults to "models/tqa/tqa_ppl.csv".
        """
        super().__init__()
        self._ppl_full = ppl_full
        self._column = column
        self._new_column = (new_column,)
        self._ppl_filepath = ppl_filepath

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes the TQA score based on the binary indicators and the assigned weights.

        Args:
            data (DataFrame): The input PySpark DataFrame containing binary indicators for filters.

        Returns:
            DataFrame: The input DataFrame with an additional column for the computed TQA score.
        """

        # Load perplexity scores
        ppl_pandas_df = IOService.read(self._ppl_filepath)
        # Convert to spark DataFrame
        ppl_spark_df = ps.DataFrame(ppl_pandas_df).to_spark()

        # Compute the weights as a list of scalar values
        weights = [
            max(0.0, (self._ppl_full - row["Perplexity"]) / self._ppl_full)
            for row in ppl_spark_df.collect()
        ]

        # Obtain the list of filter columns
        filters = [col for col in data.columns if col.startswith("tqf")]

        # Compute the weighted sum of filter indicators
        filter_sum_expr = sum(
            F.col(filter_col).cast("double") * weight
            for filter_col, weight in zip(filters, weights)
        )

        # Add the computed TQA score as a new column
        data = data.withColumn(self._new_column, filter_sum_expr)

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class TQATask3(Task):
    """
    A task to compute a final Text Quality Assessment (TQA) score by normalizing and combining two TQA scores
    using specified weights.

    Attributes:
        new_column (str): Column containing the final text quality score.
        tqa1_weight (float): The weight assigned to the first TQA score. Defaults to 0.4.
        tqa2_weight (float): The weight assigned to the second TQA score. Defaults to 0.6.
        _data (DataFrame): The DataFrame holding the data after computation.
    """

    def __init__(
        self,
        new_column: str = "enrichment_tqa_score_final",
        tqa1_weight: float = 0.4,
        tqa2_weight: float = 0.6,
    ):
        """
        Initializes the TQATask3 with specified weights for combining the two TQA scores.

        Args:
            tqa1_weight (float): Weight for the first TQA score. Defaults to 0.4.
            tqa2_weight (float): Weight for the second TQA score. Defaults to 0.6.
        """
        super().__init__()
        self._new_column = new_column
        self._tqa1_weight = tqa1_weight
        self._tqa2_weight = tqa2_weight
        self._data = None

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Normalizes two TQA scores to the range [0, 1] and computes a final TQA score
        using the specified weights.

        Args:
            data (DataFrame): The input PySpark DataFrame containing "enrichment_tqa_score1" and "enrichment_tqa_score2" columns.

        Returns:
            DataFrame: The PySpark DataFrame with normalized scores and the final combined TQA score.
        """
        # Normalize both scores to [0, 1]
        min_max_enrichment_tqa_score1 = data.select(
            F.min("enrichment_tqa_score1"), F.max("enrichment_tqa_score1")
        ).first()
        min_enrichment_tqa_score1, max_enrichment_tqa_score1 = (
            min_max_enrichment_tqa_score1
        )

        min_max_enrichment_tqa_score2 = data.select(
            F.min("enrichment_tqa_score2"), F.max("enrichment_tqa_score2")
        ).first()
        min_enrichment_tqa_score2, max_enrichment_tqa_score2 = (
            min_max_enrichment_tqa_score2
        )

        data = data.withColumn(
            "enrichment_tqa_score1",
            (F.col("enrichment_tqa_score1") - min_enrichment_tqa_score1)
            / (max_enrichment_tqa_score1 - min_enrichment_tqa_score1),
        )

        data = data.withColumn(
            "enrichment_tqa_score2",
            (F.col("enrichment_tqa_score2") - min_enrichment_tqa_score2)
            / (max_enrichment_tqa_score2 - min_enrichment_tqa_score2),
        )

        # Combine scores using weights
        data = data.withColumn(
            self._new_column,
            self._tqa1_weight * F.col("enrichment_tqa_score1")
            + self._tqa2_weight * F.col("enrichment_tqa_score2"),
        )

        self._data = data
        return data
