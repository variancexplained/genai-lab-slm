#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/incubator/flow/data_prep/feature/task.py                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Friday November 15th 2024 04:26:36 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import warnings

import textstat
import torch
from pyspark import SparkContext
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from sparknlp.annotator import PerceptronModel, Tokenizer
from sparknlp.base import DocumentAssembler, Finisher
from transformers import GPT2LMHeadModel, GPT2TokenizerFast

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
class ComputeSyntacticStatsTask(Task):
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
        Initializes the ComputeSyntacticStatsTask with the specified text or POS column.

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
class ComputeLexicalStatsTask(Task):
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
        Initializes the ComputeLexicalStatsTask with the specified text column.

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
            F.when(
                F.col("stats_char_count") > 0,
                F.col("stats_digits_count") / F.col("stats_char_count"),
            ).otherwise(0),
        )

        # 4. Punctuation count
        data = data.withColumn(
            "stats_special_chars_count",
            F.expr("length(regexp_replace(content, '[^\\p{Punct}]', ''))"),
        )

        # 5. Punctuation proportion
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
#                                    COMPUTE REVIEW AGE                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputeReviewAgeTask(Task):
    """
    A task to compute the "review age" of each entry in a specified date column of a PySpark DataFrame.

    The review age is calculated as the difference in days between the latest date in the column and each
    individual date, providing a measure of how old each review is relative to the most recent one.

    Attributes:
        column (str): The name of the date column to calculate review age from. Defaults to "date".
        feature_column (str): Statistic column to create. Default is 'eda_review_age'.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Calculates the review age for each row in the specified date column and returns the DataFrame
            with the new "eda_review_age" column.

    """

    def __init__(
        self, column: str = "date", feature_column: str = "eda_review_age"
    ) -> None:
        super().__init__()
        self._column = column
        self._feature_column = feature_column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the review age calculation on the specified date column.

        The function first identifies the maximum date within the column and then calculates the number of days
        between each review date and this maximum date, storing the result in a new "eda_review_age" column.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the specified date column.

        Returns:
            DataFrame: The input DataFrame with an additional "eda_review_age" column representing the
            review age in days.
        """
        # Step 1: Find the maximum date in the specified column
        max_date = data.agg(F.max(self._column)).first()[0]

        # Step 2: Calculate the "review age" as the difference between max_date and each row's date
        data = data.withColumn(
            self._feature_column, F.datediff(F.lit(max_date), F.col(self._column))
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
        feature_column (str): The name of the output column where the deviation will be stored. Defaults to "stats_deviation_rating".

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
        feature_column: str = "stats_deviation_rating",
    ) -> None:
        """Initializes the ComputeAggDeviationStats task with specified aggregation and target columns."""
        super().__init__()
        self._agg_by = agg_by
        self._column = column
        self._feature_column = feature_column

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
            self._feature_column,
            F.col(self._column) - F.col(f"stats_{self._column}_avg"),
        )

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
        feature_column (str): The name of the column where the readability score will be stored. Defaults to
            "readability_flesch_reading_ease".

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the readability calculation on the specified column of the input DataFrame and returns the
            DataFrame with the new readability score column.
    """

    def __init__(
        self,
        column: str = "content",
        feature_column: str = "readability_flesch_reading_ease",
    ) -> None:
        super().__init__()
        self._column = column
        self._feature_column = feature_column

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
            self._feature_column, flesch_reading_ease_udf(F.col(self._column))
        )

        data = data.fillna({self._feature_column: 60})

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA STATS TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ComputePerplexityFiltersTask(Task):
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

        # 7. Whether digits to words ratio is greater than 0.25
        data = data.withColumn(
            "tqf_high_digit_ratio", F.col("stats_digits_proportion") > 0.25
        )

        # 8. Whether punctuation to words ratio is greater than 0.25
        data = data.withColumn(
            "tqf_high_punctuation_ratio", F.col("stats_special_chars_proportion") > 0.25
        )

        # 9. Whether word count is in the range > 3 and < 256
        data = data.withColumn(
            "tqf_word_count_range",
            (F.col("stats_word_count") > 3) & (F.col("stats_word_count") < 256),
        )

        # 10. Whether readability is easy
        data = data.withColumn(
            "tqf_readability_easy", (F.col("readability_flesch_reading_ease") > 70)
        )

        # 11. Whether readability is standard to fairly difficult
        data = data.withColumn(
            "tqf_readability_std",
            (F.col("readability_flesch_reading_ease") > 50)
            & (F.col("readability_flesch_reading_ease") <= 70),
        )

        # 12. Whether readability is difficult
        data = data.withColumn(
            "tqf_readability_difficult",
            (F.col("readability_flesch_reading_ease") <= 50),
        )

        # 13. Stop wprd match
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

        # Create a new column "tqf_first_letter_cap" based on the first letter being uppercase
        data = data.withColumn(
            "tqf_first_letter_cap", F.expr("substring(content, 1, 1) rlike '^[A-Z]'")
        )

        # Create a new column "tqf_no_all_caps" based on whether the content is all caps
        data = data.withColumn("tqf_no_all_caps", ~F.col("content").rlike("^[^a-z]*$"))

        # Create a new column "tqf_high_word_repetition" if 'stats_word_repetition_ratio' >= 0.2
        data = data.withColumn(
            "tqf_high_word_repetition", F.col("stats_word_repetition_ratio") >= 0.2
        )

        # Define the regex pattern for special characters (non-alphanumeric, non-punctuation)
        special_chars_pattern = r"[^a-zA-Z0-9\s.,!?;:'\"()\-]"

        # Set tqf_no_special_chars to True if content has no special characters
        data = data.withColumn(
            "tqf_no_special_chars", ~F.col("content").rlike(special_chars_pattern)
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE PERPLEXITY TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class ComputePerplexityTask(Task):
    """
    A Spark-based task that calculates the perplexity of text data using a pre-trained GPT-2 model.
    This task is designed to run in parallel on Spark, leveraging Hugging Face's GPT-2 model for
    perplexity calculations. The model and tokenizer are broadcasted across Spark executors to
    minimize memory usage and improve performance.

    Attributes:
        column (str): The name of the DataFrame column containing text data for perplexity calculation.
        device (str): The device to run the model on, either 'cuda' or 'cpu'. Automatically set to 'cpu'
            if 'cuda' is unavailable.
        stride (int): The number of tokens to shift at each step when sliding over the text for perplexity
            calculations, based on Hugging Face's implementation.
        model_id (str): The identifier of the pre-trained model on Hugging Face's model hub.
        feature_column (str): The name of the new DataFrame column where computed perplexity values
            are stored.
        _model_broadcast (Broadcast): Broadcasted instance of the pre-trained model.
        _tokenizer_broadcast (Broadcast): Broadcasted instance of the tokenizer associated with the model.

    Methods:
        run(data: DataFrame) -> DataFrame:
            Executes the perplexity calculation for each row in the DataFrame, adding a new column with
            perplexity scores. Calls helper methods to broadcast the model and tokenizer.

        _get_spark_context() -> SparkContext:
            Retrieves the active Spark context; raises an error if no Spark session is active.

        _broadcast_model(sc: SparkContext) -> None:
            Loads the model from Hugging Face and broadcasts it across Spark executors to ensure efficient
            distribution without duplicating memory usage.

        _broadcast_tokenizer(sc: SparkContext) -> None:
            Loads the tokenizer from Hugging Face and broadcasts it across Spark executors.

        _validate() -> None:
            Validates and sets the device to 'cpu' if 'cuda' is unavailable.
    """

    def __init__(
        self,
        column: str = "content",
        device: str = "cuda",
        stride: int = 512,
        model_id: str = "openai-community/gpt2-large",
        feature_column: str = "tqa_perplexity",
    ) -> None:
        super().__init__()
        self._column = column
        self._device = device
        self._model_id = model_id
        self._stride = stride
        self._feature_column = feature_column
        self._model_broadcast = None
        self._tokenizer_broadcast = None
        self._validate()

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Runs the perplexity calculation on the provided DataFrame.

        This method retrieves the Spark context, broadcasts the model and tokenizer,
        and defines a Spark UDF to calculate perplexity for each row in the specified column.

        Args:
            data (DataFrame): The Spark DataFrame containing the text data to calculate perplexity on.

        Returns:
            DataFrame: The original DataFrame with an additional column containing perplexity scores.
        """
        sc = self._get_spark_context()
        self._broadcast_model(sc=sc)
        self._broadcast_tokenizer(sc=sc)

        @udf(FloatType())
        def compute_perplexity(text):
            # This method is derived from the HuggingFace tutorial on
            # Perplexity of fixed-length models.
            # https://huggingface.co/docs/transformers/en/perplexity

            # Obtain the model and tokenizer from spark context broadcast.
            model = self._model_broadcast.value
            tokenizer = self._tokenizer_broadcast.value

            # Tokenize text
            encodings = tokenizer(text, return_tensors="pt")

            # Obtain max length and sequence length
            max_length = model.config.n_positions
            seq_len = encodings.input_ids.size(1)

            # Hardcoding stride and device as a test dataframe writing exception
            stride = 512
            device = "cuda"

            nlls = []
            prev_end_loc = 0
            for begin_loc in range(0, seq_len, stride):
                end_loc = min(begin_loc + max_length, seq_len)
                trg_len = (
                    end_loc - prev_end_loc
                )  # may be different from stride on last loop
                input_ids = encodings.input_ids[:, begin_loc:end_loc].to(device)
                target_ids = input_ids.clone()
                target_ids[:, :-trg_len] = -100

                with torch.no_grad():
                    outputs = model(input_ids, labels=target_ids)

                    # Loss is calculated using CrossEntropyLoss, which averages over valid labels.
                    # N.B. The model only calculates loss over trg_len - 1 labels,
                    # because it internally shifts the labels to the left by 1.
                    neg_log_likelihood = outputs.loss

                nlls.append(neg_log_likelihood)

                prev_end_loc = end_loc
                if end_loc == seq_len:
                    break

            perplexity = torch.exp(torch.stack(nlls).mean())

            if torch.isnan(perplexity):
                return 0.0
            elif torch.isinf(perplexity):
                return 0.0
            else:
                return perplexity.item()

        data = data.withColumn(
            self._feature_column,
            F.when(
                F.length(data[self._column]) > 2, compute_perplexity(data[self._column])
            ).otherwise(0.0),
        )

        # Replace non-float values in the perplexity column with 0.0
        data = data.withColumn(
            self._feature_column,
            F.when(
                (F.col(self._feature_column).cast("float").isNotNull())
                & (~F.col(self._feature_column).isin(float("inf"), float("-inf"))),
                F.col(self._feature_column),
            ).otherwise(
                0.0
            ),  # Replace with 0.0 if not a valid float
        )
        return data

    def _get_spark_context(self) -> SparkContext:
        """
        Retrieves the active Spark context. If no active Spark session is found,
        raises a RuntimeError to signal the issue.

        Returns:
            SparkContext: The active Spark context.

        Raises:
            RuntimeError: If no Spark session is active.
        """
        spark = SparkSession.getActiveSession()
        if spark:
            return spark.sparkContext
        else:
            raise RuntimeError("No active Spark session found")

    def _broadcast_model(self, sc: SparkContext) -> None:
        """
        Broadcasts the pre-trained GPT-2 model to all Spark executors.

        Broadcasting minimizes memory usage by sending a single copy of the model to each executor,
        avoiding repeated instantiation and ensuring that all tasks access the same model instance.

        Args:
            sc (SparkContext): The active Spark context to perform the broadcast.
        """
        model = GPT2LMHeadModel.from_pretrained(self._model_id).to(self._device)
        self._model_broadcast = sc.broadcast(model)

    def _broadcast_tokenizer(self, sc: SparkContext) -> None:
        """
        Broadcasts the tokenizer associated with the pre-trained GPT-2 model to all Spark executors.

        Broadcasting ensures that only one instance of the tokenizer is shared across tasks,
        improving efficiency by avoiding redundant instantiation.

        Args:
            sc (SparkContext): The active Spark context to perform the broadcast.
        """
        tokenizer = GPT2TokenizerFast.from_pretrained(self._model_id)
        self._tokenizer_broadcast = sc.broadcast(tokenizer)

    def _validate(self) -> None:
        """
        Validates the device setting for model inference.

        If 'cuda' is specified as the device but is unavailable, this method sets the device to 'cpu'
        to ensure compatibility. Raises an error if the device specification is invalid.
        """
        if self._device == "cuda":
            self._device = self._device if torch.cuda.is_available() else "cpu"
