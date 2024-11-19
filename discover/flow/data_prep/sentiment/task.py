#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/sentiment/task.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Tuesday November 19th 2024 12:52:18 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Sentiment Analysis Module"""
import os
import warnings
from typing import Type

import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from discover.flow.base.task import Task
from discover.infra.config.flow import FlowConfigReader
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
tqdm.pandas()


# ------------------------------------------------------------------------------------------------ #
class SentimentAnalysisTask(Task):
    """Task for performing sentiment analysis on text data.

    This class uses a pre-trained transformer model to predict the sentiment
    of text data in a specified column, and appends the results to a new column.
    """

    def __init__(
        self,
        column="content",
        new_column="sentiment",
        cache: str = "models/sentiment/inference/sentiments",
        model_name: str = "tabularisai/robust-sentiment-analysis",
        config_reader_cls: Type[FlowConfigReader] = FlowConfigReader,
    ):
        """Initializes the SentimentAnalysisTask with configuration details.

        Args:
            column (str): The name of the column containing the text data. Defaults to "content".
            new_column (str): The name of the column to store sentiment predictions. Defaults to "sentiment".
            cache (str): The path to the cache file for saving sentiment analysis results. Defaults to "models/sentiment/inference/sentiments".
            model_name (str): The name of the pre-trained sentiment analysis model. Defaults to "tabularisai/robust-sentiment-analysis".
            config_reader_cls (Type[FlowConfigReader]): The class used to read environment-specific configuration. Defaults to FlowConfigReader.
        """
        super().__init__(
            column=column,
            new_column=new_column,
        )
        # Load environment-specific settings and construct the cache file path
        env = config_reader_cls().get_environment()
        self._cache = f"{cache}_{env}.csv"
        self._model_name = model_name

        # Model, tokenizer, and device are initialized as None and will be loaded later
        self._model = None
        self._tokenizer = None
        self._device = None

    @property
    def cache(self) -> str:
        return self._cache

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes sentiment analysis on the given DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing text data.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing sentiment predictions.
        """
        # Clear CUDA memory to ensure enough space is available for the model
        torch.cuda.empty_cache()

        # Load the device, model, and tokenizer
        self._load_model_tokenizer_to_device()

        # Apply sentiment prediction to each text entry in the specified column
        data[self._new_column] = data[self._column].progress_apply(
            self.predict_sentiment
        )
        return data

    def predict_sentiment(self, text):
        """Predicts the sentiment of a given text using the loaded model.

        Args:
            text (str): The input text for sentiment analysis.

        Returns:
            str: The predicted sentiment label.
        """
        with torch.no_grad():
            # Tokenize and prepare the input text for the model
            inputs = self._tokenizer(
                text.lower(),
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512,
            )
            # Move inputs to the appropriate device (CPU or GPU)
            inputs = {key: value.to(self._device) for key, value in inputs.items()}
            # Get model outputs and calculate probabilities
            outputs = self._model(**inputs)
            probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)

            # Determine the predicted class
            predicted_class = torch.argmax(probabilities, dim=-1).item()

        # Map the predicted class index to a sentiment label
        sentiment_map = {
            0: "Very Negative",
            1: "Negative",
            2: "Neutral",
            3: "Positive",
            4: "Very Positive",
        }
        return sentiment_map[predicted_class]

    def _load_model_tokenizer_to_device(self) -> None:
        """Loads the device, tokenizer, and model for sentiment analysis."""
        # Select GPU if available, otherwise use CPU
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Load the tokenizer and model from the pre-trained model name
        self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._model_name
        )
        # Move the model to the selected device
        self._model.to(self._device)
