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
# Modified   : Monday November 18th 2024 04:18:12 am                                               #
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
    ) -> None:

        super().__init__()
        self._weight_noun = weight_noun
        self._weight_adjective = weight_adjective
        self._weight_verb = weight_verb
        self._weight_adverb = weight_adverb

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
            # Step 1: Calculate the total token count per review.
            data = data.withColumn("tqa_token_count", F.size("tp_tokens"))

            # Step 2: Calculate the unique token count per review.
            data = data.withColumn(
                "tqa_unique_token_count", F.size(F.array_distinct("tp_tokens"))
            )

            # Step 3: Compute the Lexical Diversity Score (TTR).
            data = data.withColumn(
                "tqa_lexical_diversity",
                F.col("tqa_unique_token_count") / F.col("tqa_token_count"),
            )

            # Step 4: Calculate counts of specific POS tags
            pos_tags = ["NOUN", "VERB", "ADJ", "ADV"]
            pos_labels = ["nouns", "verbs", "adjectives", "adverbs"]
            for i, tag in enumerate(pos_tags):
                try:
                    data = data.withColumn(
                        f"pos_n_{pos_labels[i]}",
                        F.expr(f"size(filter(tp_pos, x -> x = '{tag}'))"),
                    )
                except AnalysisException as e:
                    raise AnalysisException(f"Error processing POS tag '{tag}': {e}")

            # Step 5: Calculate ratios/percentages of specific POS tags
            for i, tag in enumerate(pos_tags):
                try:
                    data = data.withColumn(
                        f"pos_p_{pos_labels[i]}",
                        F.when(
                            F.col("tqa_token_count") > 0,
                            F.col(f"pos_n_{pos_labels[i]}") / F.col("tqa_token_count"),
                        ).otherwise(0),
                    )
                except AnalysisException as e:
                    raise AnalysisException(
                        f"Error calculating ratio for POS tag '{tag}': {e}"
                    )

            # Step 6: Compute the Syntactic Score
            data = data.withColumn(
                "tqa_syntactic_score",
                (
                    self._weight_noun * F.col("pos_p_nouns")
                    + self._weight_adjective * F.col("pos_p_adjectives")
                    + self._weight_verb * F.col("pos_p_verbs")
                    + self._weight_adverb * F.col("pos_p_adverbs")
                ),
            )

            # Step 7: Compute the Length-Weighted Text Quality Score
            data = data.withColumn(
                "tqa_score",
                F.col("tqa_syntactic_score") * F.log1p(F.col("tqa_token_count")),
            )

            for i, tag in enumerate(pos_tags):
                # Drop the intermediate POS count and proportion columns
                data = data.drop(f"pos_n_{pos_labels[i]}")
                data = data.drop(f"pos_p_{pos_labels[i]}")

            # Delete tokens and POS tags from dataset
            data = data.drop("tp_tokens", "tp_pos")

            return data

        except AnalysisException as e:
            raise AnalysisException(f"Column 'tp_pos' not found: {e}")
        except Exception as e:
            raise Exception(f"Unexpected error during POS statistics calculation: {e}")
