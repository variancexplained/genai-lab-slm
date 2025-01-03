#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/archive/flow/task/enrich/tqa.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 7th 2024 11:03:10 pm                                              #
# Modified   : Friday January 3rd 2025 01:01:17 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Module"""
import os

import seaborn as sns
from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
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

    def __init__(self, column: str = "content", **kwargs) -> None:
        """
        Initializes NLPTask with the column to process.

        Parameters
        ----------
        column : str, optional
            The name of the column containing the content data to process (default is "content").
        """
        super().__init__(**kwargs)
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
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self._weight_noun = weight_noun
        self._weight_adjective = weight_adjective
        self._weight_verb = weight_verb
        self._weight_adverb = weight_adverb
        self._weight_syntactic = weight_syntactic
        self._weight_lexical = weight_lexical

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Computes POS statistics and text quality scores, emphasizing raw POS counts.

        Args:
            data (DataFrame): Input PySpark DataFrame with POS tags in "tp_pos" and tokens in "tp_tokens".

        Returns:
            DataFrame: Output DataFrame with text quality scores and additional POS count columns.
        """
        try:
            # Step 1: Calculate the total token count per review
            data = data.withColumn("tqa_token_count", F.size("tp_tokens"))

            # Step 2: Calculate the unique token count per review
            data = data.withColumn(
                "tqa_unique_token_count", F.size(F.array_distinct("tp_tokens"))
            )

            # Step 3: Compute Lexical Diversity (TTR)
            data = data.withColumn(
                "tqa_lexical_diversity",
                F.col("tqa_unique_token_count") / F.col("tqa_token_count"),
            )

            # Step 4: Calculate counts of specific POS tags
            pos_tags = ["NOUN", "VERB", "ADJ", "ADV"]
            pos_labels = ["nouns", "verbs", "adjectives", "adverbs"]

            for i, tag in enumerate(pos_tags):
                data = data.withColumn(
                    f"tqa_pos_n_{pos_labels[i]}",
                    F.expr(f"size(filter(tp_pos, x -> x = '{tag}'))"),
                )

            # Step 5: Compute Syntactic Score based on raw POS counts
            data = data.withColumn(
                "tqa_syntactic_score",
                (
                    self._weight_noun * F.col("tqa_pos_n_nouns")
                    + self._weight_adjective * F.col("tqa_pos_n_adjectives")
                    + self._weight_verb * F.col("tqa_pos_n_verbs")
                    + self._weight_adverb * F.col("tqa_pos_n_adverbs")
                ),
            )

            # Step 6: Combine Syntactic and Lexical Scores
            data = data.withColumn(
                "tqa_score",
                (
                    self._weight_syntactic * F.col("tqa_syntactic_score")
                    + self._weight_lexical * F.col("tqa_lexical_diversity")
                ),
            )

            # Step 7: Log transform to make the distribution more symmetric
            data = data.withColumn("tqa_score", F.log1p(F.col("tqa_score")))

            # # Step 8: Scale scores to [0, 100]
            # data = data.withColumn(
            #     "tqa_score",
            #     (
            #         (F.col("tqa_score") - F.min("tqa_score").over(Window.partitionBy()))
            #         / (
            #             F.max("tqa_score").over(Window.partitionBy())
            #             - F.min("tqa_score").over(Window.partitionBy())
            #         )
            #         * 100
            #     ),
            # )

            # Drop unnecessary columns
            data = data.drop("tp_tokens", "tp_pos")

            return data
        except Exception as e:
            raise RuntimeError(f"Error in ComputeTextQualityTask: {e}")
