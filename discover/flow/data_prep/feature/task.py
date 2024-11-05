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
# Modified   : Monday November 4th 2024 11:32:22 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import warnings

from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sparknlp.annotator import PerceptronModel, Tokenizer
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
