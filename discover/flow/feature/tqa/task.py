#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/feature/tqa/task.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:03:10 pm                                              #
# Modified   : Wednesday November 20th 2024 02:24:44 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Module"""
import os

import seaborn as sns
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from sparknlp.annotator import PerceptronModel, Tokenizer
from sparknlp.base import DocumentAssembler, Finisher

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
sns.set_style("white")
sns.set_palette("Blues_r")
# ------------------------------------------------------------------------------------------------ #
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


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
class ComputeTextQualityTask(Task):

    def __init__(
        self,
        weight_noun: float = 0.3,
        weight_adjective: float = 0.3,
        weight_verb: float = 0.2,
        weight_adverb: float = 0.2,
        weight_syntactic: float = 0.5,
        weight_lexical: float = 0.5,
    ) -> None:

        super().__init__()
        self._weight_noun = weight_noun
        self._weight_adjective = weight_adjective
        self._weight_verb = weight_verb
        self._weight_adverb = weight_adverb
        self._weight_syntactic = weight_syntactic
        self._weight_lexical = weight_lexical

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes POS statistics and text quality scores for aspect-based sentiment analysis.

        This method calculates various POS-related statistics, including counts and proportions of
        specific POS tags (nouns, verbs, adjectives, adverbs) within each review. It also computes
        the Lexical Diversity Score (TTR) and combines it with a length-weighted Syntactic Score
        to derive a comprehensive Text Quality Score. The method then drops intermediate columns
        used in the calculations.

        Args:
            data (DataFrame): The input PySpark DataFrame containing the POS tags as a list
                            in the "tp_pos" column and tokens in the "tp_tokens" column.

        Returns:
            DataFrame: The input DataFrame with additional columns:
                    - "tqa_token_count": Total token count per review.
                    - "tqa_unique_token_count": Unique token count per review.
                    - "tqa_lexical_diversity": Lexical diversity score (TTR).
                    - "tqa_syntactic_score": Length-weighted syntactic score.
                    - "tqa_score": Final combined text quality score.
        """
        try:
            # Step 1: Calculate the total token count per review
            data = data.withColumn("tqa_token_count", F.size("tp_tokens"))

            # Step 2: Calculate the unique token count per review
            data = data.withColumn(
                "tqa_unique_token_count", F.size(F.array_distinct("tp_tokens"))
            )

            # Step 3: Compute the Lexical Diversity Score (TTR)
            data = data.withColumn(
                "tqa_lexical_diversity",
                F.col("tqa_unique_token_count") / F.col("tqa_token_count"),
            )

            # Step 4: Calculate counts and proportions of specific POS tags
            pos_tags = ["NOUN", "VERB", "ADJ", "ADV"]
            pos_labels = ["nouns", "verbs", "adjectives", "adverbs"]

            for i, tag in enumerate(pos_tags):
                data = data.withColumn(
                    f"pos_n_{pos_labels[i]}",
                    F.expr(f"size(filter(tp_pos, x -> x = '{tag}'))"),
                )
                data = data.withColumn(
                    f"pos_p_{pos_labels[i]}",
                    F.when(
                        F.col("tqa_token_count") > 0,
                        F.col(f"pos_n_{pos_labels[i]}") / F.col("tqa_token_count"),
                    ).otherwise(0),
                )

            # Step 5: Compute the Syntactic Score
            data = data.withColumn(
                "tqa_syntactic_score_intermediate",
                (
                    self._weight_noun * F.col("pos_p_nouns")
                    + self._weight_adjective * F.col("pos_p_adjectives")
                    + self._weight_verb * F.col("pos_p_verbs")
                    + self._weight_adverb * F.col("pos_p_adverbs")
                ),
            )

            # Step 6: Compute the Length-Weighted Syntactic Score
            data = data.withColumn(
                "tqa_syntactic_score",
                F.col("tqa_syntactic_score_intermediate")
                * F.log1p(F.col("tqa_token_count")),
            )

            # Step 7: Combine syntactic and lexical measures
            data = data.withColumn(
                "tqa_score",
                (
                    self._weight_syntactic * F.col("tqa_syntactic_score")
                    + self._weight_lexical * F.col("tqa_lexical_diversity")
                ),
            )

            # Step 8: Set quality of non-english reviews to 0
            data = data.withColumn(
                "tqa_score",
                F.when(F.col("dqd_non_english_text") == True, 0).otherwise(  # noqa
                    F.col("tqa_score")
                ),
            )

            # Step 9: Scale the values to [0,100]
            data = data.withColumn(
                "tqa_score",
                (
                    (F.col("tqa_score") - F.min("tqa_score").over(Window.partitionBy()))
                    / (
                        F.max("tqa_score").over(Window.partitionBy())
                        - F.min("tqa_score").over(Window.partitionBy())
                    )
                    * 100
                ),
            )

            # Step 10: Clean up intermediate columns
            for label in pos_labels:
                data = data.drop(f"pos_n_{label}", f"pos_p_{label}")

            # Drop unnecessary columns
            data = data.drop("tp_tokens", "tp_pos")

            return data

        except AnalysisException as e:
            raise AnalysisException(f"Column 'tp_pos' not found: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error during POS statistics calculation: {e}")
