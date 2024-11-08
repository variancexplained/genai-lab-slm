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
# Modified   : Friday November 8th 2024 01:34:54 am                                                #
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


class SentimentClassifierTask(Task):
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
        new_column: str = "enrich_sentiment",
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
class SpacySentimentClassifierTask(SentimentClassifierTask):
    """
    A sentiment classification task using spaCy for natural language processing.

    This class inherits from SentimentClassifierTask and uses a spaCy pipeline
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
        self._pipeline = pipeline
        self._nlp = spacy.load(self._pipeline)

    def classify(self, text):
        """
        Classifies the sentiment of the given text using the spaCy NLP pipeline.

        Args:
            text: The text to classify.

        Returns:
            float: The accumulated sentiment score of the text based on token vectors.
        """
        doc = self._nlp(text)
        sentiment = 0
        for token in doc:
            if token.has_vector and token.vector_norm > 0:
                sentiment += token.sentiment
        return sentiment


# ------------------------------------------------------------------------------------------------ #
class TextBlobSentimentClassifierTask(SentimentClassifierTask):
    """
    A sentiment classification task using TextBlob for natural language processing.

    This class inherits from SentimentClassifierTask and uses TextBlob to classify
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
