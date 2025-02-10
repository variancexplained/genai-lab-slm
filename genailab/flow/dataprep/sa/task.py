#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/sa/task.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:53:03 am                                                #
# Modified   : Saturday February 8th 2025 10:43:01 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Sentiment Analysis Task Module"""
import os
import warnings
from typing import Type

import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger
from genailab.infra.utils.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
tqdm.pandas()


# ------------------------------------------------------------------------------------------------ #
class SentimentClassificationTask(Task):
    """
    Task for performing sentiment analysis on text data in a specified column of a Pandas DataFrame.

    This task uses a pre-trained model to predict sentiment for text in the specified column and
    stores the sentiment predictions in a new column. Results are cached to a file to avoid reprocessing.
    It supports execution on GPUs or local devices depending on the configuration.

    Args:
        cache_filepath (str): Path to the cache file for storing or loading sentiment predictions.
        column (str): The name of the column in the DataFrame containing text data for sentiment analysis.
            Defaults to "content".
        new_column (str): The name of the column to store sentiment predictions. Defaults to "sentiment".
        model_name (str): The name of the pre-trained model to use for sentiment analysis. Defaults to
            "tabularisai/robust-sentiment-analysis".
        device_local (bool): Indicates whether to execute the task on local devices. Defaults to False.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the sentiment analysis task, using a cache if available. If not, it predicts sentiment
            for the text column and caches the results.
        predict_sentiment(text: str) -> str:
            Predicts sentiment for a given text string.
        _load_model_tokenizer_to_device() -> None:
            Loads the model, tokenizer, and device for performing sentiment analysis.
        _run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the model inference for sentiment prediction and writes the results to the cache.
    """

    def __init__(
        self,
        cache_filepath: str,
        column="content",
        new_column="sentiment",
        model_name: str = "tabularisai/robust-sentiment-analysis",
        device_local: bool = False,
        io_cls: Type[IOService] = IOService,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._column = column
        self._new_column = f"{self.stage.id}_{new_column}"
        self._model_name = model_name
        self._cache_filepath = cache_filepath
        self._device_local = device_local
        self._io = io_cls()

        # Model, tokenizer, and device are initialized as None and will be loaded later
        self._model = None
        self._tokenizer = None
        self._device = None

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the sentiment analysis task on the input DataFrame.

        This method first attempts to read sentiment predictions from a cache file. If the cache
        is not available or not valid, it performs sentiment analysis using the pre-trained model
        and writes the results to the cache. Sentiment predictions are stored in the specified
        `new_column` of the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with sentiment predictions added to the specified column.

        Raises:
            FileNotFoundError: If the cache is not found or the task is run locally without a GPU.
            Exception: For any other unexpected errors.
        """
        try:
            cache = self._io.read(filepath=self._cache_filepath, lineterminator="\n")
            cache["id"] = cache["id"].astype("string")
            data = data.merge(cache[["id", self._new_column]], how="left", on="id")
            return data
        except (FileNotFoundError, TypeError):
            if self._device_local:
                return self._run(data=data)
            else:
                msg = (
                    f"Cache not found or not available. {self.__class__.__name__} is not "
                    "supported on local devices. Try running on Kaggle, Colab, or AWS."
                )
                self._logger.error(msg)
                raise FileNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception encountered.\n{e}"
            self._logger.exception(msg)
            raise

    def _run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes model inference for sentiment analysis and writes results to the cache.

        This method processes the input DataFrame by applying sentiment predictions for each entry
        in the specified text column. It uses parallel processing for efficient computation and
        writes the results to the cache file.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text data.

        Returns:
            pd.DataFrame: The DataFrame with sentiment predictions added to the specified column.
        """
        torch.cuda.empty_cache()  # Clear CUDA memory to ensure sufficient space

        # Load the device, model, and tokenizer
        self._load_model_tokenizer_to_device()

        # Apply sentiment prediction to each text entry
        data[self._new_column] = data[self._column].progress_apply(
            self.predict_sentiment
        )

        # Write results to the cache file
        self._write_file(
            filepath=self._cache_filepath, data=data[["id", self._new_column]]
        )

        return data

    def predict_sentiment(self, text: str) -> str:
        """
        Predicts the sentiment of a given text string.

        This method uses the loaded model and tokenizer to predict the sentiment of the input
        text. It maps the model's output to a sentiment label.

        Args:
            text (str): The input text string.

        Returns:
            str: The predicted sentiment label, e.g., "Positive", "Negative", or "Neutral".
        """
        with torch.no_grad():
            inputs = self._tokenizer(
                text.lower(),
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=512,
            )
            inputs = {key: value.to(self._device) for key, value in inputs.items()}
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

    def _load_model_tokenizer_to_device(self) -> None:
        """
        Loads the pre-trained model, tokenizer, and device for sentiment analysis.

        This method selects the appropriate device (GPU or CPU), loads the tokenizer and model
        based on the specified model name, and moves the model to the selected device.
        """
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self._tokenizer = AutoTokenizer.from_pretrained(self._model_name)
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._model_name
        )
        self._model.to(self._device)