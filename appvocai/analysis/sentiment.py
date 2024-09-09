#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/analysis/sentiment.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 4th 2024 12:33:13 am                                                   #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Sentiment Analysis Module"""
from __future__ import annotations

import logging
import os
import random
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, NamedTuple, Union

import evaluate
import numpy as np
import pandas as pd
from appvocai.analysis.config import (
    SentimentClassifierDataManagerConfig,
    SentimentClassifierFineTuningConfig,
    SentimentClassifierTrainingConfig,
)
from appvocai.analysis.data import SentimentClassifierDataManager
from appvocai.callbacks.callbacklist import CallbackList
from appvocai.data.review import ReviewDataset
from appvocai.shared.utils.print import print_dict
from appvocai.utils.data import DataClass
from appvocai.utils.version import Version
from datasets import Dataset, DatasetDict
from dotenv import load_dotenv
from peft import LoraConfig, get_peft_model
from tqdm import tqdm
from transformers import (
    AutoModelForSequenceClassification,
    PreTrainedModel,
    Trainer,
    TrainingArguments,
    pipeline,
)

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
logging.basicConfig(level=logging.DEBUG)


# ------------------------------------------------------------------------------------------------ #
# pylint: disable=line_too_long
# ------------------------------------------------------------------------------------------------ #
class SentimentClassifierSelfTrained:
    """Analyze sentiment using semi-supervised learning on unlabeled reviews.

    Args:
        filepath (str): Path to reviews csv file.
        model_name (str):
        review_col_name (str): Column name in reviews dataset containing review text.
        model_name (str): The name of the model used for training and fine-tuning.
        train_config (TrainingConfig): Configuration for HuggingFace Trainer





    """

    __TASK = "sentiment-analysis"

    def __init__(
        self,
        reviews: ReviewDataset,
        base_model: str,
        datamanager_config: SentimentClassifierDataManagerConfig,
        train_config: SentimentClassifierTrainingConfig,
        finetune_config: SentimentClassifierFineTuningConfig,
        callbacks: list = None,
        metric_name: str = "accuracy",
    ) -> None:
        self._reviews = reviews
        self._base_model = base_model
        self._datamanager_config = datamanager_config
        self._train_config = train_config
        self._finetune_config = finetune_config
        self._callbacks = callbacks if callbacks else []
        self._metric_name = metric_name

        self._review_samples = None
        self._stop_session = False
        self._logs = {}
        self._name = None

        self._model = None
        self._peft_model = None
        self._lora_config = None
        self._trainer = None
        self._tokenizer = None
        self._data_collator = None
        self._metric = None
        self._datamanager = None
        self._history = None

        self._version = Version().get_version(entity=self.__TASK)

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def project(self) -> str:
        return os.getenv("HUGGINGFACE_PROJECT")

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, batch_size: int) -> None:
        self._name = f"sentiment-analysis-lora-{batch_size}_{self._version}"

    @property
    def model(self) -> PreTrainedModel:
        return self._model

    @property
    def output_dir(self) -> str:
        directory = os.getenv("MODEL_DIRECTORY")
        return f"{directory}/{self.__TASK}/{self.name}"

    @property
    def model_repo(self) -> str:
        return f"{self.project}/{self.__TASK}/{self.name}"

    @property
    def stop_session(self) -> bool:
        return self._stop_session

    @stop_session.setter
    def stop_session(self, stop: bool) -> None:
        self._stop_session = stop

    @property
    def history(self) -> pd.DataFrame:
        return self._history

    @history.setter
    def history(self, history: pd.DataFrame) -> None:
        if self._history:
            self._history = pd.concat([self._history, history], axis=0)
        else:
            self._history = history

    def initialize_session(self) -> None:
        """Instantiates models, tokenizers, data collators and metric objects."""
        # Sample the review dataset.
        self._review_samples = self._reviews.sample(
            frac=self._datamanager_config.dataset_size
        )

        # Instantiate the Low-Rank Adaptation (LoRA) PEFT configuration
        self._lora_config = LoraConfig(**self._finetune_config.as_dict())

        # Base model for sentiment analysis
        model = AutoModelForSequenceClassification.from_pretrained(
            pretrained_model_name_or_path=self._base_model
        )
        # Construct the PeftModel based on the base model and the LoRA config.
        self._peft_model = get_peft_model(model, self._lora_config)  # Training
        self._model = self._peft_model  # Inference
        self._model = self._model.merge_and_unload()

        # Instantiate the Data Manager
        self._datamanager = SentimentClassifierDataManager(
            model_name=self._base_model, config=self._datamanager_config
        )
        # Obtain the tokenizer from the datamanager
        self._tokenizer = self._datamanager.tokenizer

        # The Data Collator converts the training samples to PyTorch tensors for faster training
        self._data_collator = self._datamanager.data_collator

        # Object used during the training process to compute a performance metric
        self._metric = evaluate.load(self._metric_name)

        # Instantiate callbacks
        self._callbacklist = CallbackList(callbacks=self._callbacks, model=self)
        self._callbacklist.set_params(params=self._finetune_config.as_dict())

    def get_reviews(self) -> Dataset:
        """Obtain reviews from the ReviewDataset and return a Dataset object."""

        review_samples = self._reviews.sample(
            frac=self._datamanager_config.dataset_size,
            stratify=["category_id", "rating"],
            columns=["app_id", "category_id", "rating", "content"],
            shuffle=self._datamanager_config.shuffle_data,
        )

        # Convert the DataFrame to a Dataset object for batch processing.
        dataset = self._datamanager.create_dataset(examples=review_samples)

        return dataset

    def predict(self, examples: pd.DataFrame) -> list:
        """Performs inference on review data

        Args:
            examples (pd.DataFrame): DataFrame containing review data

        Returns:
            List of dictionaries. Each dictionary contains an example's predicted label and score.
        """

        pipe = pipeline(
            task=self.__TASK,
            tokenizer=self._datamanager.tokenizer,
            model=self._model,
        )
        tokenizer_kwargs = {"truncation": True}
        predictions = pipe(
            examples[self._datamanager_config.review_col], **tokenizer_kwargs
        )

        self._logger.info(f"Predict method returning {len(predictions)} predictions.")

        return predictions

    def compute_metrics(self, eval_pred: NamedTuple) -> dict:
        """Computes the evaluation metric

        Args:
            eval_pred (NamedTuple): Contains predictions in terms of logits and labels

        """
        logits, labels = eval_pred
        predictions = np.argmax(logits, axis=-1)
        return self._metric.compute(predictions=predictions, references=labels)

    def train(self, tokenized_datasets: DatasetDict) -> None:
        """Train the model on the prepared datasets

        Args:
            datasets (DatasetDict): Dictionary containing tokenized training and test sets.
        """

        self._train_config.output_dir = self.output_dir
        training_args = TrainingArguments(**self._train_config.as_dict())

        self._trainer = Trainer(
            model=self._peft_model,
            args=training_args,
            train_dataset=tokenized_datasets["train"],
            eval_dataset=tokenized_datasets["test"],
            tokenizer=self._tokenizer,
            data_collator=self._data_collator,
            compute_metrics=self.compute_metrics,
        )

        # Keep version of peft model in inference mode.
        self._model = self._peft_model
        self._model = self._model.merge_and_unload()

        self._logger.info(
            f"Training started on {len(tokenized_datasets['train'])} observations."
        )
        self._trainer.train()
        self._logger.info("Training complete")

    def evaluate(self, tokenized_datasets: DatasetDict) -> dict:
        """Runs an evaluation on the training and test data and updates the logs.

        Args:
            tokenized_datasets (DatasetDict): DatasetDict containing training
                and test (validation) set.

        Returns:
            Dictionary of metrics
        """
        scores = self._trainer.evaluate(
            eval_dataset=tokenized_datasets, metric_key_prefix=""
        )
        scores = {k.replace("_train", "train"): v for k, v in scores.items()}
        scores = {k.replace("_validation", "validation"): v for k, v in scores.items()}
        scores = {k.replace("_test", "test"): v for k, v in scores.items()}

        self._logger.debug(f"\n{print_dict(data=scores, title='Scores')}")
        self._logger.debug(f"\n{scores}")

        self._logs["train_accuracy"] = scores["train_accuracy"]
        self._logs["validation_accuracy"] = scores["validation_accuracy"]
        self._logs["test_accuracy"] = scores["test_accuracy"]

        return scores

    def save_model(self) -> None:
        """Saves the model and predictions to a designated directory"""
        self._trainer.save_model(output_dir=self._output_dir)
        self._logger.info(f"Saved model to {self._output_dir}.")

    def _gather_dataset_statistics(self, tokenized_datasets: DatasetDict) -> None:
        # Set the session name object and add the name, and dataset size to the logs
        dataset_size = (
            len(tokenized_datasets["train"])
            + len(tokenized_datasets["validation"])
            + len(tokenized_datasets["test"])
        )

        self.name = dataset_size
        self._logs["name"] = self.name
        self._logs["dataset_size"] = dataset_size

    def finetune_batch(self, batch) -> None:
        """Processes the fine tuning batch

        Args:
            batch (Dataset): Dataset object containing a batch of reviews.

        """

        # Convert the batch to a DataFrame object for ease of manipulation
        batch_df = pd.DataFrame.from_dict(batch, orient="index").T

        # Obtain pseudolabels for the batch
        predictions = self.predict(examples=batch)

        # Create tokenized training and test sets.
        tokenized_datasets = self._datamanager.get_tokenized_batch(
            examples=batch_df, predictions=predictions
        )

        # Update the log statistics
        self._gather_dataset_statistics(tokenized_datasets=tokenized_datasets)

        # Notify the callbacks that training has begun
        self._callbacklist.on_train_begin(logs=self._logs)

        # Train the model
        self.train(tokenized_datasets=tokenized_datasets)

        # Evaluate the model
        scores = self.evaluate(tokenized_datasets=tokenized_datasets)

        # Notify callbacks that training has ended.
        self._callbacklist.on_train_end(logs=self._logs)

        # The EarlyStop callback sets the stop_session if performance
        # hasn't improved.
        if self.stop_session:
            self._exit_session()

    def run(self) -> None:
        """Semi-supervised sentiment classification driver method"""

        # Initialize the HuggingFace tokenizer, data collator, and various objects.
        self.initialize_session()

        # Obtain the data from the ReviewDataset
        dataset = self.get_reviews()

        # Create 10 batches
        batch_size = int(len(dataset[self._datamanager_config.review_col]) / 10)

        # Notify callbacks that the session has begun.
        self._callbacklist.on_session_begin()

        # Process the fine-tuning session in batches
        dataset.map(self.finetune_batch, batched=True, batch_size=batch_size)

        # Notify callbacks that the session has ended.
        self._callbacklist.on_session_end()

        self.save_model()

        self._exit_session()

    def _exit_session(self) -> None:
        print(self._history)
        print(f"=" * 80)
        sys.exit(0)
