#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/sentiment/task.py                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Saturday November 16th 2024 07:41:34 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import warnings
from abc import abstractmethod
from typing import Tuple, Type

import numpy as np
import pandas as pd
import spacy
import torch
from pandarallel import pandarallel
from textblob import TextBlob
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from discover.flow.base.task import Task
from discover.infra.config.app import AppConfigReader
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
pandarallel.initialize(nb_workers=18, progress_bar=True, verbose=False)


# ------------------------------------------------------------------------------------------------ #
class SentimentAnalysisTask(Task):
    """
    A base class for performing sentiment analysis on text data in a pandas DataFrame.

    This class is designed to handle sentiment analysis efficiently by using parallel processing
    to score and label text data. It requires subclasses to implement the `analyze` method.

    Attributes:
        column (str): The name of the column containing the text data for sentiment analysis.
        new_column (str): The prefix for the new columns that will store the sentiment score and label.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "an_sentiment",
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the sentiment analysis task on the provided DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data for sentiment analysis.

        Returns:
            pd.DataFrame: The updated DataFrame with two new columns: one for the sentiment score
                          and one for the sentiment label.
        """
        score_column = f"{self._new_column}_score"
        label_column = f"{self._new_column}_label"

        # Apply the analyze method in parallel and create a new DataFrame
        results_df = pd.DataFrame(
            data[self._column].parallel_apply(self.analyze),
            columns=[score_column, label_column],
            index=data.index,
        )

        # Concatenate the original data with the new results DataFrame
        data = pd.concat([data, results_df], axis=1)
        return data

    @abstractmethod
    def analyze(self, text) -> Tuple[float, str]:
        """
        Abstract method to perform sentiment analysis on a given text.

        This method should be implemented by subclasses to define the sentiment analysis logic.

        Args:
            text (str): The input text to analyze.

        Returns:
            Tuple[float, str]: A tuple containing the sentiment score (as a float) and the
                               sentiment label (as a string).
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class SentimentScoreTask(Task):
    """
    A base class for performing sentiment analysis on a pandas DataFrame.

    Attributes:
        column (str): The name of the column containing text data for sentiment analysis.
        new_column (str): The prefix for the new columns that store the sentiment score and label.
        min_score (float): The minimum possible sentiment score.
        max_score (float): The maximum possible sentiment score.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "an_sentiment",
        min_score: float = -1.0,
        max_score: float = 1.0,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._min_score = min_score
        self._max_score = max_score

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the sentiment analysis task on the provided DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data for sentiment analysis.

        Returns:
            pd.DataFrame: The updated DataFrame with sentiment scores and labels added.
        """
        score_column = f"{self._new_column}_score"
        label_column = f"{self._new_column}_label"

        data[score_column] = data[self._column].parallel_apply(self.score)
        data[label_column] = data[score_column].parallel_apply(self.classify)
        return data

    @abstractmethod
    def score(self, text) -> float:
        """
        Abstract method to calculate the sentiment score for a given text.

        Args:
            text: The input text for which the sentiment score is calculated.

        Returns:
            Union[float, str]: The sentiment score as a float or a string representation.
        """
        pass

    def classify(self, score: float) -> str:
        """
        Classifies a sentiment score into one of the predefined sentiment labels.

        Args:
            score (float): The sentiment score to classify.

        Returns:
            str: The sentiment label corresponding to the score.

        Raises:
            IndexError: If the score falls outside the expected range, indicating a configuration issue.
        """
        labels = ["Very Negative", "Negative", "Neutral", "Positive", "Very Positive"]
        intervals = np.linspace(self._min_score, self._max_score, len(labels) + 1)
        condition = intervals > score
        idx = np.min(np.where(condition)) - 1

        try:
            return labels[idx]
        except IndexError as e:
            msg = (
                "An index error occurred. This likely indicates an incorrect minimum or "
                "maximum sentiment score set in the configuration file. "
                "Check the documentation for the classifier you are using.\n"
                f"{e}"
            )
            self._logger.error(msg)
            raise


# ------------------------------------------------------------------------------------------------ #
class SentimentLabelTask(Task):
    """
    A base class for performing sentiment classification on text data in a pandas DataFrame.

    This class provides a framework for sentiment classification, allowing subclasses to implement
    their own sentiment classification logic. It uses parallel processing to efficiently classify
    large datasets.

    Attributes:
        column (str): The name of the column containing the text data for classification.
        new_column (str): The prefix for the new column that will store the sentiment label.
        min_score (float): The minimum sentiment score (for reference, even if not directly used).
        max_score (float): The maximum sentiment score (for reference, even if not directly used).
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "an_sentiment",
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the sentiment classification task on the provided DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data for classification.

        Returns:
            pd.DataFrame: The updated DataFrame with a new column containing sentiment labels.
        """
        label_column = f"{self._new_column}_label"
        data[label_column] = data[self._column].parallel_apply(self.classify)
        return data

    @abstractmethod
    def classify(self, text: str) -> str:
        """
        Abstract method to perform sentiment classification on a given text.

        This method should be implemented by subclasses to define the logic for
        sentiment classification.

        Args:
            text (str): The input text to classify.

        Returns:
            str: The sentiment label for the given text.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class SpacySentimentAnalysisTask(SentimentScoreTask):
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
        new_column="an_sentiment",
        pipeline: str = "en_core_web_sm",
        min_score: float = -1.0,
        max_score: float = 1.0,
    ):
        super().__init__(
            column=column,
            new_column=new_column,
            min_score=min_score,
            max_score=max_score,
        )
        from spacytextblob.spacytextblob import SpacyTextBlob  # noqa

        self._pipeline = pipeline
        self._nlp = spacy.load(self._pipeline)
        self._nlp.add_pipe("spacytextblob")

    def score(self, text):
        """
        Classifies the sentiment of the given text using the spaCy NLP pipeline.

        Args:
            row: The row to classify.

        Returns:
            float: The accumulated sentiment score of the text based on token vectors.
        """
        return self._nlp(text)._.blob.polarity


# ------------------------------------------------------------------------------------------------ #
class TextBlobSentimentAnalysisTask(SentimentScoreTask):
    """
    A sentiment classification task using TextBlob for natural language processing.

    This class inherits from SentimentAnalysisTask and uses TextBlob to classify
    the sentiment of text data. The `classify` method returns the polarity score
    provided by TextBlob's sentiment analysis.

    Attributes:
        column (str): The name of the column containing the text data to be classified.
        new_column (str): The name of the column where the sentiment results will be stored.
    """

    def __init__(
        self,
        column="content",
        new_column="an_sentiment",
        min_score: float = -1.0,
        max_score: float = 1.0,
    ):
        super().__init__(
            column=column,
            new_column=new_column,
            min_score=min_score,
            max_score=max_score,
        )

    def score(self, text):
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
class VaderSentimentAnalysisTask(SentimentAnalysisTask):
    """
    A task for performing sentiment analysis using VADER.

    This class uses the VADER SentimentIntensityAnalyzer to analyze the sentiment
    of text data and compute sentiment scores, adding the results to the DataFrame.

    Attributes:
        column (str): The name of the DataFrame column containing the text to be analyzed.
        _analyzer (SentimentIntensityAnalyzer): An instance of VADER's SentimentIntensityAnalyzer.
    """

    def __init__(self, column="content", **kwargs):
        self._column = column
        self._analyzer = SentimentIntensityAnalyzer()

    def analyze(self, text):
        """
        Analyzes the sentiment of a given piece of text using VADER.

        Args:
            text (str): The text to analyze.

        Returns:
            pd.Series: A Series containing:
                - `compound_score` (float): The compound sentiment score.
                - `highest_class` (str): The sentiment class with the highest proportion.
        """
        if pd.isna(text):
            return pd.Series([0.0, "neu"])
        scores = self._analyzer.polarity_scores(text)
        compound_score = scores["compound"]

        # Find the sentiment class with the highest proportion
        highest_class = max(["neg", "neu", "pos"], key=lambda k: scores[k])

        return compound_score, highest_class


# ------------------------------------------------------------------------------------------------ #
class MergeSentimentsTask(Task):
    """
    A task to merge sentiment analysis results with the main dataset.

    Attributes:
        location (str): The file location of the sentiment data.
        file_prefix (str): The prefix used for the sentiment data files.
        config_reader_cls (type): The class used to read configuration settings.
        io_cls (type): The class used to handle file input/output operations.
    """

    def __init__(
        self,
        new_column: str = "an_sentiment",
        location: str = "models/sentiment/inference",
        file_prefix: str = "sentiments_",
        config_reader_cls: Type[AppConfigReader] = AppConfigReader,
        io_cls: Type[IOService] = IOService,
    ) -> None:
        """
        Initializes the MergeSentimentsTask with the specified parameters.

        Args:
            location (str): The directory where sentiment data files are stored. Defaults to "models/sentiment/inference".
            file_prefix (str): The prefix for sentiment data files. Defaults to "sentiments_".
            config_reader_cls (type): The class used for reading configuration settings. Defaults to AppConfigReader.
            io_cls (type): The class used for I/O operations. Defaults to IOService.
        """
        super().__init__()
        self._new_column = new_column
        self._location = location
        self._file_prefix = file_prefix
        self._config_reader = config_reader_cls()
        self._io = io_cls()

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Merges sentiment analysis data with the main dataset.

        Args:
            data (pd.DataFrame): The main dataset to which sentiment data will be merged.

        Returns:
            pd.DataFrame: The updated dataset with sentiment data merged on the "id" column.
        """
        sentiments = self._get_sentiment_data()
        if len(sentiments) != len(data):
            msg = f"sentiment with {len(sentiments)} rows is not compatible with data with {len(data)} rows."
            self._logger.error(msg)
            raise ValueError(msg)

        data = data.merge(sentiments[["id", "sentiment"]], how="left", on="id")
        data = data.rename(columns={"sentiment": self._new_column})
        return data

    def _get_sentiment_data(self) -> pd.DataFrame:
        """
        Reads the sentiment data from a file based on the current environment.

        Returns:
            pd.DataFrame: The sentiment data as a DataFrame.

        Raises:
            FileNotFoundError: If the sentiment data file is not found.
            pd.errors.ParserError: If there is an issue parsing the CSV file.
        """
        env = self._config_reader.get_environment()
        filename = f"{self._file_prefix}{env}.csv"
        filepath = os.path.join(self._location, filename)

        try:
            df = self._io.read(filepath=filepath, lineterminator="\n")
            df["id"] = df["id"].astype(str)
            return df
        except FileNotFoundError:
            self._logger.error(f"Sentiment data file not found: {filepath}")
            raise
        except pd.errors.ParserError as e:
            self._logger.error(f"Error parsing sentiment data file: {filepath}\n{e}")
            raise


# ------------------------------------------------------------------------------------------------ #
class DistilBERTSentimentClassifier(SentimentLabelTask):
    """
    A sentiment classification task using spaCy for natural language processing.

    This class inherits from SentimentAnalysisTask and uses a spaCy pipeline
    to classify the sentiment of text data. The `classify` method calculates
    sentiment based on the vectors and sentiment attributes of tokens.

    Attributes:
        column (str): The name of the column containing the text data to be classified.
        new_column (str): The name of the column where the sentiment results will be stored.
    """

    def __init__(
        self,
        column="content",
        new_column="an_sentiment",
        model_name: str = "tabularisai/robust-sentiment-analysis",
    ):
        super().__init__(
            column=column,
            new_column=new_column,
        )
        self._model_name = model_name
        self._model = None
        self._tokenizer = None

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the sentiment analysis task on the provided DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data for sentiment analysis.

        Returns:
            pd.DataFrame: The updated DataFrame with sentiment scores and labels added.
        """
        label_column = f"{self._new_column}_label"

        self._load_model_tokenizer()

        data[label_column] = data[self._column].parallel_apply(self.classify)
        return data

    def classify(self, text):
        """
        Classifies the sentiment of the given text using the spaCy NLP pipeline.

        Args:
            row: The row to classify.

        Returns:
            float: The accumulated sentiment score of the text based on token vectors.
        """
        inputs = self._tokenizer(
            text.lower(),
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=512,
        )
        with torch.no_grad():
            outputs = self._model(**inputs)

        probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)
        predicted_class = torch.argmax(probabilities, dim=-1).item()

        sentiment_map = {
            0: "Very Negative",
            1: "Negative",
            2: "Neutral",
            3: "Positive",
            4: "Very Positive",
        }
        return sentiment_map[predicted_class]

    def _load_model_tokenizer(self) -> None:
        self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._model_name
        )
