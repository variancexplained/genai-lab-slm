#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/tqa/task.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:03:10 pm                                              #
# Modified   : Sunday November 17th 2024 01:12:54 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Module"""
import math
import os

import pandas as pd
import pyspark.pandas as ps
import seaborn as sns
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sparknlp.annotator import PerceptronModel, Tokenizer
from sparknlp.base import DocumentAssembler, Finisher

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
sns.set_style("white")
sns.set_palette("Blues_r")
# ------------------------------------------------------------------------------------------------ #
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


# ------------------------------------------------------------------------------------------------ #
#                                       TQA TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class TQATask(Task):
    """
    Base class for all Text Quality Analysis (TQA) tasks.

    This class provides a foundation for TQA tasks, automatically prefixing the specified
    new column name with the stage ID to ensure consistency in naming.

    Attributes
    ----------
    new_column : str
        The name of the new column to be added, prefixed with the stage ID.

    Parameters
    ----------
    new_column : str, optional
        The base name of the new column. The full column name will be prefixed
        with the stage ID to create a unique, stage-specific identifier.
    """

    def __init__(self, new_column: str = None) -> None:
        super().__init__()
        self._new_column = f"{self.stage_id}_{new_column}"


# ------------------------------------------------------------------------------------------------ #
#                                       NLP TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class NLPTask(TQATask):
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
class ComputeSyntacticStatsTask(TQATask):
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
        try:
            # Step 1: Calculate the total POS tag count per review.
            data = data.withColumn("pos_count", F.size("tp_pos"))

            # Step 2: Calculate counts of specific POS tags
            pos_tags = ["NOUN", "VERB", "ADJ", "ADV", "DET"]
            pos_labels = ["nouns", "verbs", "adjectives", "adverbs", "determiners"]
            for i, tag in enumerate(pos_tags):
                try:
                    data = data.withColumn(
                        f"pos_n_{pos_labels[i]}",
                        F.expr(f"size(filter(tp_pos, x -> x = '{tag}'))"),
                    )
                except AnalysisException as e:
                    raise AnalysisException(f"Error processing POS tag '{tag}': {e}")

            # Step 3: Calculate ratios/percentages of specific POS tags
            for i, tag in enumerate(pos_tags):
                try:
                    data = data.withColumn(
                        f"pos_p_{pos_labels[i]}",
                        F.when(
                            F.col("pos_count") > 0,
                            F.col(f"pos_n_{pos_labels[i]}") / F.col("pos_count"),
                        ).otherwise(0),
                    )
                except AnalysisException as e:
                    raise AnalysisException(
                        f"Error calculating ratio for POS tag '{tag}': {e}"
                    )

            # Drop intermediate column if not needed
            data = data.drop("pos_count")

            return data

        except AnalysisException as e:
            raise AnalysisException(f"Column 'tp_pos' not found: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error during POS statistics calculation: {e}")


# ------------------------------------------------------------------------------------------------ #
#                              COMPUTE LEXICAL STATS                                               #
# ------------------------------------------------------------------------------------------------ #
class ComputeLexicalStatsTask(TQATask):
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
        try:
            # Ensure the column exists
            try:
                data = data.withColumn("stats_char_count", F.length(self._column))
            except AnalysisException as e:
                raise AnalysisException(
                    f"Column '{self._column}' not found in DataFrame: {str(e)}"
                )

            # 1. Character count
            try:
                data = data.withColumn("stats_char_count", F.length(self._column))
            except Exception as e:
                raise ValueError(
                    f"Error calculating character count for column '{self._column}': {str(e)}"
                )

            # 2. Digits count
            try:
                data = data.withColumn(
                    "stats_digits_count", F.expr("regexp_count(content, '[^0-9]')")
                )
            except Exception as e:
                raise ValueError(f"Error calculating digits count: {str(e)}")

            # 3. Digits proportion
            try:
                data = data.withColumn(
                    "stats_digits_proportion",
                    F.when(
                        F.col("stats_char_count") > 0,
                        F.col("stats_digits_count") / F.col("stats_char_count"),
                    ).otherwise(0),
                )
            except Exception as e:
                raise ValueError(f"Error calculating digits proportion: {str(e)}")

            # 4. Special chars count
            try:
                data = data.withColumn(
                    "stats_special_chars_count",
                    F.expr("regexp_count(content, r'[^\\w\\s]')"),
                )
            except Exception as e:
                raise ValueError(f"Error calculating special chars count: {str(e)}")

            # 5. Special chars proportion
            try:
                data = data.withColumn(
                    "stats_special_chars_proportion",
                    F.when(
                        F.col("stats_char_count") > 0,
                        F.col("stats_special_chars_count") / F.col("stats_char_count"),
                    ).otherwise(0),
                )
            except Exception as e:
                raise ValueError(
                    f"Error calculating special chars proportion: {str(e)}"
                )

            # 6. Punctuation count
            try:
                data = data.withColumn(
                    "stats_punctuation_count",
                    F.expr("regexp_count(content, '[.,!?;:]')"),
                )
            except Exception as e:
                raise ValueError(f"Error calculating punctuation count: {str(e)}")

            # 7. Punctuation proportion
            try:
                data = data.withColumn(
                    "stats_punctuation_proportion",
                    F.when(
                        F.col("stats_char_count") > 0,
                        F.col("stats_punctuation_count") / F.col("stats_char_count"),
                    ).otherwise(0),
                )
            except Exception as e:
                raise ValueError(f"Error calculating punctuation proportion: {str(e)}")

            # 8. Split content into words
            try:
                data = data.withColumn("words", F.split(F.col(self._column), "\\s+"))
            except Exception as e:
                raise ValueError(f"Error splitting content into words: {str(e)}")

            # 9. Word count
            try:
                data = data.withColumn("stats_word_count", F.size("words"))
            except Exception as e:
                raise ValueError(f"Error calculating word count: {str(e)}")

            # 10. Unique word count
            try:
                data = data.withColumn("unique_words", F.array_distinct("words"))
                data = data.withColumn(
                    "stats_unique_word_count", F.size("unique_words")
                )
            except Exception as e:
                raise ValueError(f"Error calculating unique word count: {str(e)}")

            # 11. Unique word proportion
            try:
                data = data.withColumn(
                    "stats_unique_word_proportion",
                    F.when(
                        F.col("stats_word_count") > 0,
                        F.col("stats_unique_word_count") / F.col("stats_word_count"),
                    ).otherwise(0),
                )
            except Exception as e:
                raise ValueError(f"Error calculating unique word proportion: {str(e)}")

            # 12. Word repetition ratio
            try:
                data = data.withColumn(
                    "stats_word_repetition_ratio",
                    1 - F.col("stats_unique_word_proportion"),
                )
            except Exception as e:
                raise ValueError(f"Error calculating word repetition ratio: {str(e)}")

            # Drop intermediate columns
            try:
                data = data.drop("words", "unique_words")
            except Exception as e:
                raise ValueError(f"Error dropping intermediate columns: {str(e)}")

            # 13. Word length statistics
            try:
                data = data.withColumn(
                    "word_lengths",
                    F.expr("transform(split(content, '\\\\s+'), x -> length(x))"),
                )

                # Minimum word length
                data = data.withColumn(
                    "stats_word_length_min", F.array_min("word_lengths")
                )

                # Maximum word length
                data = data.withColumn(
                    "stats_word_length_max", F.array_max("word_lengths")
                )

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
            except Exception as e:
                raise ValueError(f"Error calculating word length statistics: {str(e)}")

            return data

        except AnalysisException as e:
            raise AnalysisException(
                f"Column '{self._column}' not found in DataFrame: {str(e)}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during statistics calculation: {str(e)}"
            )


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE 1 TASK                                         #
# ------------------------------------------------------------------------------------------------ #
class ComputeSyntacticLexicalScoresTask(TQATask):
    """
    A task to compute a Text Quality Assessment (TQA) score based on various components
    such as POS count, POS diversity, lexical complexity, POS intensity, and TQA quality checks.

    Attributes:
        pos_diversity_weight (float): The weight assigned to the POS diversity component.
        pos_density_weight (float): The weight assigned to the POS intensity component.
        lexical_complexity_weight (float): The weight assigned to the lexical complexity component.
        column (str): Column containing review text.
        new_column (str): The name of the output column to store the computed TQA score.
    """

    def __init__(
        self,
        pos_diversity_weight: float,
        pos_density_weight: float,
        lexical_complexity_weight: float,
        column: str = "content",
        new_column: str = "syntactic_lexical_score",
    ) -> None:
        """
        Initializes the ComputeSyntacticLexicalScoresTask with specified weights and output column name.

        Args:
            pos_diversity_weight (float): Weight for the POS diversity component.
            pos_density_weight (float): Weight for the POS intensity component.
            lexical_complexity_weight (float): Weight for the lexical complexity component.
            new_column (str): Name of the output column for the TQA score. Defaults to "enrichment_tqa_score1".
        """
        super().__init__(new_column=new_column)
        self._pos_diversity_weight = pos_diversity_weight
        self._pos_density_weight = pos_density_weight
        self._lexical_complexity_weight = lexical_complexity_weight
        self._column = column

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the TQA score computation by applying several components as UDFs.

        This method calculates the following:
        - POS diversity score: Measures the diversity of parts of speech used.
        - POS density score: Quantifies the density of POS tags in the content.
        - Lexical complexity score: Measures the complexity of the language used.

        Args:
            data (DataFrame): The input PySpark DataFrame containing text data and related features.

        Returns:
            DataFrame: The input DataFrame with additional columns for each component score and the final TQA score.
        """
        try:
            # Ensure the required text column exists
            if self._column not in data.columns:
                raise AnalysisException(
                    f"Column '{self._column}' not found in DataFrame"
                )

            # Define UDFs for each computation
            @F.udf("float")
            def compute_pos_diversity_score(
                content, pos_p_nouns, pos_p_verbs, pos_p_adjectives, pos_p_adverbs
            ):
                """
                Computes the POS diversity score using an entropy-based calculation.

                The POS diversity score is computed based on the proportions of different
                parts of speech in the content, with a higher score indicating more variety
                in the types of POS tags used.

                Args:
                    content (str): The text content of the review.
                    pos_p_nouns (float): Proportion of nouns in the content.
                    pos_p_verbs (float): Proportion of verbs in the content.
                    pos_p_adjectives (float): Proportion of adjectives in the content.
                    pos_p_adverbs (float): Proportion of adverbs in the content.

                Returns:
                    float: The computed POS diversity score.
                """
                try:
                    if content and len(content) > 2:
                        pos_tags = [
                            pos_p_nouns,
                            pos_p_verbs,
                            pos_p_adjectives,
                            pos_p_adverbs,
                        ]
                        pos_diversity = -sum(p * math.log(p) for p in pos_tags if p > 0)
                        return float(pos_diversity * self._pos_diversity_weight)
                    return 0.0
                except Exception as e:
                    raise ValueError(f"Error in compute_pos_diversity_score: {str(e)}")

            @F.udf("float")
            def compute_pos_density_score(
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

                This score quantifies the density of specific parts of speech in the content,
                with a higher score indicating a more "dense" use of those parts of speech.

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
                try:
                    if content and len(content) > 2 and stats_word_count > 0:
                        pos_density = (
                            pos_n_nouns + pos_n_verbs + pos_n_adjectives + pos_n_adverbs
                        ) / stats_word_count
                        return float(pos_density * self._pos_density_weight)
                    return 0.0
                except Exception as e:
                    raise ValueError(f"Error in compute_pos_density_score: {str(e)}")

            @F.udf("float")
            def compute_lexical_complexity_score(
                content,
                stats_unique_word_proportion,
                stats_special_chars_proportion,
                stats_word_length_std,
            ):
                """
                Computes the lexical complexity score based on unique word proportion,
                special character proportion, and word length standard deviation.

                This score measures the complexity of the language used in the content,
                with a higher score indicating more complexity (e.g., more unique words,
                special characters, or varied word lengths).

                Args:
                    content (str): The text content of the review.
                    stats_unique_word_proportion (float): Proportion of unique words in the content.
                    stats_special_chars_proportion (float): Proportion of special characters in the content.
                    stats_word_length_std (float): Standard deviation of word lengths in the content.

                Returns:
                    float: The computed lexical complexity score.
                """
                try:
                    if content and len(content) > 2:
                        lexical_complexity = (
                            0.4 * stats_unique_word_proportion
                            + 0.3 * stats_special_chars_proportion
                            + 0.3 * stats_word_length_std
                        )
                        return float(
                            lexical_complexity * self._lexical_complexity_weight
                        )
                    return 0.0
                except Exception as e:
                    raise ValueError(
                        f"Error in compute_lexical_complexity_score: {str(e)}"
                    )

            # Apply UDFs to create new columns
            try:
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
                    "tqm_pos_density_score",
                    compute_pos_density_score(
                        F.col(self._column),
                        F.col("pos_n_nouns"),
                        F.col("pos_n_verbs"),
                        F.col("pos_n_adjectives"),
                        F.col("pos_n_adverbs"),
                        F.col("stats_word_count"),
                    ),
                )
                data = data.withColumn(
                    "tqm_lexical_complexity_score",
                    compute_lexical_complexity_score(
                        F.col(self._column),
                        F.col("stats_unique_word_proportion"),
                        F.col("stats_special_chars_proportion"),
                        F.col("stats_word_length_std"),
                    ),
                )
            except Exception as e:
                raise RuntimeError(
                    f"Error applying UDFs to calculate lexical complexity score: {str(e)}"
                )

            # Calculate the TQA score as a weighted combination of components
            try:
                data = data.withColumn(
                    self._new_column,
                    F.col("tqm_pos_diversity_score")
                    + F.col("tqm_lexical_complexity_score")
                    + F.col("tqm_pos_density_score"),
                )
            except Exception as e:
                raise RuntimeError(
                    f"Error calculating lexical complexity score: {str(e)}"
                )

            return data

        except AnalysisException as e:
            raise AnalysisException(
                f"Column '{self._column}' not found in DataFrame: {str(e)}"
            )
        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during lexical complexity score calculation: {str(e)}"
            )


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE PERPLEXITY FILTERS  TASK                                   #
# ------------------------------------------------------------------------------------------------ #
class ComputePerplexityFiltersTask(TQATask):
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

    def __init__(
        self,
    ) -> None:
        super().__init__()

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes a series of transformations on the input DataFrame to compute various
        textual quality features (TQF) based on parts of speech and other textual statistics.

        The following TQF features are computed:
        - Whether the review has at least one adjective, adverb, determiner, noun, or verb
        - Whether the review contains terminal punctuation (., !, ?)
        - Whether the special characters to word ratio is greater than 0.25
        - Whether the punctuation to word ratio is greater than 0.25
        - Whether the word count is within a specified range
        - Whether stop words appear in the review
        - Whether the first letter is capitalized
        - Whether the content is in all caps
        - Whether the word repetition ratio is high
        - Whether the review contains special characters

        Args:
            data (DataFrame): The input PySpark DataFrame containing text data and related features.

        Returns:
            DataFrame: The input DataFrame with additional columns for each computed TQF feature.
        """
        try:
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

            # 7. Whether special characters to words ratio is greater than 0.25
            data = data.withColumn(
                "tqf_high_special_chars_ratio",
                F.col("stats_special_chars_proportion") > 0.25,
            )

            # 8. Whether punctuation to words ratio is greater than 0.25
            data = data.withColumn(
                "tqf_high_punctuation_ratio",
                F.col("stats_punctuation_proportion") > 0.25,
            )

            # 9. Whether word count is in the range > 3 and < 256
            data = data.withColumn(
                "tqf_word_count_range",
                (F.col("stats_word_count") > 3) & (F.col("stats_word_count") < 256),
            )

            # 10. Stop word match
            # List of stop words to search for
            stop_words = ["the", "be", "to", "of", "and", "that", "have", "with"]

            # Create conditions for each stop word
            conditions = [
                F.expr(f"array_contains(split(content, ' '), '{word}')").cast("int")
                for word in stop_words
            ]

            # Sum the conditions and check if at least 2 stop words are present
            data = data.withColumn(
                "tqf_stop_word_match",
                F.when(sum(conditions) >= 2, True).otherwise(False),
            )

            # 11. Create a new column "tqf_first_letter_cap" based on the first letter being uppercase
            data = data.withColumn(
                "tqf_first_letter_cap",
                F.expr("substring(content, 1, 1) rlike '^[A-Z]'"),
            )

            # 12. Create a new column "tqf_no_all_caps" based on whether the content is all caps
            data = data.withColumn(
                "tqf_no_all_caps", ~F.col("content").rlike("^[^a-z]*$")
            )

            # 13. Create a new column "tqf_high_word_repetition" if 'stats_word_repetition_ratio' >= 0.2
            data = data.withColumn(
                "tqf_high_word_repetition", F.col("stats_word_repetition_ratio") >= 0.2
            )

            # Define the regex pattern for special characters (non-alphanumeric, non-punctuation)
            special_chars_pattern = r"[^a-zA-Z0-9\s.,!?;:'\"()\-]"

            # Set tqf_no_special_chars to True if content has no special characters
            data = data.withColumn(
                "tqf_no_special_chars", ~F.col("content").rlike(special_chars_pattern)
            )

            # Delete tokens and POS tags from dataset
            data = data.drop("tp_tokens", "tp_pos")

            return data

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during perplexity filter computation: {str(e)}"
            )


# ------------------------------------------------------------------------------------------------ #
#                               COMPUTE PERPLEXITY WEIGHTS TASK                                    #
# ------------------------------------------------------------------------------------------------ #
class ComputePerplexityWeights(TQATask):
    """
    A class to compute and save perplexity weights for various filters in a PySpark DataFrame.

    This task calculates the overall average perplexity and the average perplexity for each
    filter column that starts with a specified prefix. It then computes weights for each filter
    and writes the results to a specified file.

    Attributes:
        _column (str): The name of the column containing perplexity values.
        _pp_filepath (str): The file path to save the perplexity weights.
        _pp_filter_prefix (str): The prefix used to identify filter columns.
    """

    def __init__(
        self,
        column: str = "an_perplexity",
        pp_filepath: str = "models/tqa/pp_weights.csv",
        pp_filter_prefix: str = "tqf_",
    ) -> None:
        """
        Initializes the ComputePerplexityWeights class with the specified parameters.

        Args:
            column (str): The name of the column containing perplexity values. Defaults to 'an_perplexity'.
            pp_filepath (str): The file path to save the perplexity weights. Defaults to 'models/tqa/pp_weights.csv'.
            pp_filter_prefix (str): The prefix used to identify filter columns. Defaults to 'tqf_'.
        """
        self._column = column
        self._pp_filepath = pp_filepath
        self._pp_filter_prefix = pp_filter_prefix

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes and saves perplexity weights for filter columns in the DataFrame.

        The method calculates the overall average perplexity and the average perplexity
        for each filter column. It then computes a weight for each filter based on the
        difference between the overall and individual perplexity values, and saves the
        weights to a specified file.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the perplexity values and filter columns.

        Returns:
            DataFrame: The input DataFrame (unmodified).
        """
        try:
            if not os.path.exists(self._pp_filepath):
                # Calculate the overall average perplexity
                pp_all = data.agg(F.avg(self._column)).first()[0]
                if pp_all is None:
                    raise ValueError(
                        f"Could not calculate the overall average perplexity for column '{self._column}'"
                    )

                w_i = {}

                # Get all columns that start with the filter prefix and sort them
                filters = [
                    col
                    for col in data.columns
                    if col.startswith(self._pp_filter_prefix)
                ]
                filters = sorted(filters)

                # Compute the average perplexity and the weights for each filter
                for filter in filters:
                    try:
                        filtered_data = data.filter(F.col(filter) == True)  # noqa
                        avg_perplexity_i = filtered_data.agg(
                            F.avg(self._column)
                        ).first()[0]
                        if avg_perplexity_i is None:
                            raise ValueError(
                                f"Could not calculate the average perplexity for filter '{filter}'"
                            )

                        w_i[filter] = (pp_all - avg_perplexity_i) / pp_all

                    except Exception as filter_exception:
                        raise RuntimeError(
                            f"Error calculating perplexity for filter '{filter}': {str(filter_exception)}"
                        )

                # Convert the weights dictionary to a DataFrame and write to file
                weights = pd.DataFrame.from_dict(
                    w_i, orient="index", columns=["weight"]
                ).reset_index(names="filter")
                try:
                    IOService.write(data=weights, filepath=self._pp_filepath)
                except Exception as write_exception:
                    raise RuntimeError(
                        f"Error writing perplexity weights to file '{self._pp_filepath}': {str(write_exception)}"
                    )

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during perplexity weight computation: {str(e)}"
            )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE 2 TASK                                         #
# ------------------------------------------------------------------------------------------------ #
class ComputeCoherenceScoreTask(TQATask):
    """
    A task to compute a Text Quality Assessment (TQA) coherence score based on binary filter indicators
    and pre-computed perplexity weights.

    This task reads perplexity weights from a specified file, computes the weighted sum of binary filter indicators,
    normalizes the result, and adds the computed TQA coherence score as a new column to the input DataFrame.

    Attributes:
        _new_column (str): The name of the new column to store the computed TQA coherence score.
        _pp_filepath (str): The file path to the CSV containing perplexity weights.
    """

    def __init__(
        self,
        new_column: str = "coherence_score",
        pp_filepath: str = "models/tqa/pp_weights.csv",
    ):
        super().__init__(new_column=new_column)
        self._pp_filepath = pp_filepath

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes the TQA score based on the binary indicators and the assigned weights.

        This method loads perplexity weights from a CSV file, computes the weighted sum of binary filter indicators,
        normalizes the result by the total weight, and adds the computed TQA score to the DataFrame.

        Args:
            data (DataFrame): The input PySpark DataFrame containing binary indicators for filters.

        Returns:
            DataFrame: The input DataFrame with an additional column for the computed TQA score.

        Raises:
            FileNotFoundError: If the perplexity weights file cannot be loaded.
            ValueError: If the total weight is zero, preventing TQA score computation.
            RuntimeError: If an unexpected error occurs during TQA score computation.
        """
        try:
            # Load perplexity weights from the specified file
            try:
                self._weights_pandas = IOService.read(self._pp_filepath).reset_index(
                    drop=True
                )
            except Exception as e:
                raise FileNotFoundError(
                    f"Failed to load perplexity weights from '{self._pp_filepath}': {str(e)}"
                )

            # Convert the Pandas DataFrame to a Spark DataFrame
            weights_spark = ps.DataFrame(self._weights_pandas).to_spark()

            # Collect weights into a list of dictionaries
            weights_list = weights_spark.collect()

            # Compute the total sum of the weights
            total_weight = sum(item["weight"] for item in weights_list)

            if total_weight == 0:
                raise ValueError("The total weight is zero. Cannot compute TQA score.")

            # Compute the weighted sum of filter indicators
            filter_sum_expr = sum(
                [
                    F.col(item["filter"]).cast("double") * F.lit(item["weight"])
                    for item in weights_list
                ]
            ) / F.lit(
                total_weight
            )  # Normalize by the total sum of the weights

            # Add the computed TQA score as a new column
            data = data.withColumn(self._new_column, filter_sum_expr)

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during TQA score computation: {str(e)}"
            )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                 COMPUTE TQA SCORE TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ComputeTextQualityScore(TQATask):
    """
    A task to compute a final Text Quality Assessment (TQA) score by normalizing and combining two TQA scores
    using specified weights.

    Attributes:
        new_column (str): Column containing the final text quality score.
        syntactic_weight (float): The weight assigned to the first TQA score. Defaults to 0.4.
        perplexity_weight (float): The weight assigned to the second TQA score. Defaults to 0.6.
        _data (DataFrame): The DataFrame holding the data after computation.
    """

    def __init__(
        self,
        new_column: str = "score",
        syntactic_weight: float = 0.4,
        perplexity_weight: float = 0.6,
    ):
        """
        Initializes the ComputeTextQualityScore with specified weights for combining the two TQA scores.

        Args:
            new_column (str): The name of the output column for the final TQA score. Defaults to "tqa_score".
            tqa_syntactic_weight (float): Weight for the first TQA score. Defaults to 0.4.
            tqa_perplexity_weight (float): Weight for the second TQA score. Defaults to 0.6.
        """
        super().__init__(new_column=new_column)
        self._syntactic_weight = syntactic_weight
        self._perplexity_weight = perplexity_weight

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Normalizes two TQA scores to the range [0, 1] and computes a final TQA score
        using the specified weights.

        Args:
            data (DataFrame): The input PySpark DataFrame containing "tqa_syntactic_lexical_score"
                              and "tqa_coherence_score" columns.

        Returns:
            DataFrame: The PySpark DataFrame with normalized scores and the final combined TQA score.
        """
        try:
            # Normalize both scores to [0, 1]
            min_max_enrichment_tqa_score1 = data.select(
                F.min(f"{self.stage_id}_syntactic_lexical_score"),
                F.max(f"{self.stage_id}_syntactic_lexical_score"),
            ).first()

            if min_max_enrichment_tqa_score1[1] == min_max_enrichment_tqa_score1[0]:
                raise ValueError(
                    "Syntactic lexical score has no variation. Cannot normalize."
                )

            min_enrichment_tqa_score1, max_enrichment_tqa_score1 = (
                min_max_enrichment_tqa_score1
            )

            min_max_enrichment_tqa_score2 = data.select(
                F.min(f"{self.stage_id}_coherence_score"),
                F.max(f"{self.stage_id}_coherence_score"),
            ).first()

            if min_max_enrichment_tqa_score2[1] == min_max_enrichment_tqa_score2[0]:
                raise ValueError("Coherence score has no variation. Cannot normalize.")

            min_enrichment_tqa_score2, max_enrichment_tqa_score2 = (
                min_max_enrichment_tqa_score2
            )

            # Apply Min-Max normalization to both scores
            data = data.withColumn(
                f"{self.stage_id}_syntactic_lexical_score",
                (
                    F.col(f"{self.stage_id}_syntactic_lexical_score")
                    - min_enrichment_tqa_score1
                )
                / (max_enrichment_tqa_score1 - min_enrichment_tqa_score1),
            )

            data = data.withColumn(
                f"{self.stage_id}_coherence_score",
                (F.col(f"{self.stage_id}_coherence_score") - min_enrichment_tqa_score2)
                / (max_enrichment_tqa_score2 - min_enrichment_tqa_score2),
            )

            # Combine scores using weights
            data = data.withColumn(
                self._new_column,
                self._syntactic_weight
                * F.col(f"{self.stage_id}_syntactic_lexical_score")
                + self._perplexity_weight * F.col(f"{self.stage_id}_coherence_score"),
            )

            # Compute min and max of the new column
            min_value = data.agg(F.min(self._new_column)).collect()[0][0]
            max_value = data.agg(F.max(self._new_column)).collect()[0][0]

            if max_value == min_value:
                raise ValueError(
                    f"Final TQA score has no variation. Min and max values are both {min_value}."
                )

            # Apply Min-Max transformation to the final TQA score
            data = data.withColumn(
                self._new_column,
                (F.col(self._new_column) - min_value) / (max_value - min_value),
            )

        except Exception as e:
            raise RuntimeError(
                f"Unexpected error during TQA score computation: {str(e)}"
            )

        return data
