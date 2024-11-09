#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/enrich/sentiment/task.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Friday November 8th 2024 09:28:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import warnings
from abc import abstractmethod
from typing import Union

import pandas as pd
import spacy
from pandarallel import pandarallel
from textblob import TextBlob

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=18, progress_bar=True, verbose=False)


class SentimentAnalysisTask(Task):
    """
    A base class for sentiment classification tasks.

    This class extends the Task class and provides an abstract method
    `classify` for implementing custom sentiment classification. It also
    includes a `run` method for applying the classification logic to a
    specified column in a pandas DataFrame.

    Attributes:
        column (str): The name of the column containing the text data to be classified.
        new_column (str): The name of the column where the sentiment results will be stored.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "enrichment_sentiment",
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the sentiment classification to each entry in the specified column
        of the DataFrame and stores the result in a new column.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with an additional column for sentiment results.
        """
        data[self._new_column] = data[self._column].parallel_apply(self.classify)
        return data

    @abstractmethod
    def classify(self, text) -> Union[float, str]:
        """
        Abstract method for classifying the sentiment of a given text.

        Args:
            text: The text to classify.

        Returns:
            Union[float, str]: The sentiment score or label. The specific type and
            format depend on the implementation.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class SpacySentimentAnalysisTask(SentimentAnalysisTask):
    """
    A sentiment classification task using spaCy for natural language processing.

    This class inherits from SentimentAnalysisTask and uses a spaCy pipeline
    to classify the sentiment of text data. The `classify` method calculates
    sentiment based on the vectors and sentiment attributes of tokens.

    Attributes:
        column (str): The name of the column containing the text data to be classified.
        new_column (str): The name of the column where the sentiment results will be stored.
        pipeline (str): The name of the spaCy model pipeline to load for NLP tasks.
    """

    def __init__(
        self,
        column="content",
        new_column="enrichment_sentiment",
        pipeline: str = "en_core_web_sm",
    ):
        super().__init__(column=column, new_column=new_column)
        from spacytextblob.spacytextblob import SpacyTextBlob  # noqa

        self._pipeline = pipeline
        self._nlp = spacy.load(self._pipeline)
        self._nlp.add_pipe("spacytextblob")

    def classify(self, text):
        """
        Classifies the sentiment of the given text using the spaCy NLP pipeline.

        Args:
            row: The row to classify.

        Returns:
            float: The accumulated sentiment score of the text based on token vectors.
        """
        return self._nlp(text)._.blob.polarity


# ------------------------------------------------------------------------------------------------ #
class TextBlobSentimentAnalysisTask(SentimentAnalysisTask):
    """
    A sentiment classification task using TextBlob for natural language processing.

    This class inherits from SentimentAnalysisTask and uses TextBlob to classify
    the sentiment of text data. The `classify` method returns the polarity score
    provided by TextBlob's sentiment analysis.

    Attributes:
        column (str): The name of the column containing the text data to be classified.
        new_column (str): The name of the column where the sentiment results will be stored.
    """

    def __init__(self, column="content", new_column="enrichment_sentiment"):
        super().__init__(column=column, new_column=new_column)

    def classify(self, text):
        """
        Classifies the sentiment of the given text using TextBlob.

        Args:
            text: The text to classify.

        Returns:
            float: The polarity score of the text, ranging from -1.0 (negative sentiment)
            to 1.0 (positive sentiment).
        """
        return TextBlob(text).sentiment.polarity


# ------------------------------------------------------------------------------------------------ #
class SentimentClassificationTask(Task):
    def __init__(
        self,
        column: str = "enrichment_sentiment",
        new_column: str = "enrichment_sentiment_classification",
        min_sentiment: float = -1.0,
        max_sentiment: float = 1.0,
    ):
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._min_sentiment = min_sentiment
        self._max_sentiment = max_sentiment
        self._range_span = max_sentiment - min_sentiment
        self._third_size = self._range_span / 3

        # Compute the range and thresholds in the constructor
        range_start = -1
        range_end = 1
        range_span = range_end - range_start

        # Calculate the size of each third
        third_size = range_span / 3

        # Compute thresholds
        self._negative_threshold = range_start + third_size  # -0.33
        self._positive_threshold = range_end - third_size  # 0.33

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the sentiment classification to each entry in the specified column
        of the DataFrame and stores the result in a new column.

        Args:
            data (pd.DataFrame): The input DataFrame containing the sentiment scores.

        Returns:
            pd.DataFrame: The DataFrame with an additional column for sentiment classification.
        """
        data[self._new_column] = data[self._column].parallel_apply(self.classify)
        return data

    def classify(self, score) -> str:
        # Classification logic using precomputed thresholds
        if score < self._negative_threshold:
            return "negative"
        elif score > self._positive_threshold:
            return "positive"
        else:
            return "neutral"
