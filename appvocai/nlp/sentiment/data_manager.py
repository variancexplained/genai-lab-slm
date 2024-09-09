#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/nlp/sentiment/data_manager.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 7th 2024 01:23:43 am                                                    #
# Modified   : Monday September 9th 2024 09:41:26 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import logging

import pandas as pd
from appvocai.nlp.sentiment.config import SentimentClassifierDataManagerConfig
from appvocai.shared.utils.print import print_dict
from datasets import Dataset, DatasetDict
from spellchecker import SpellChecker
from tokenizers import Tokenizer
from transformers import AutoTokenizer, DataCollatorWithPadding


# ------------------------------------------------------------------------------------------------ #
class SentimentClassifierDataManager:
    """Manages data for the Sentiment Analysis Classifier Process."""

    def __init__(self, model_name: str, config: SentimentClassifierDataManagerConfig):
        self._tokenizer = AutoTokenizer.from_pretrained(model_name)
        self._config = config
        self._spell_checker = SpellChecker()
        self._labeled_data = pd.DataFrame()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def data_collator(self) -> DataCollatorWithPadding:
        return DataCollatorWithPadding(tokenizer=self._tokenizer)

    @property
    def tokenizer(self) -> Tokenizer:
        return self._tokenizer

    def tokenize(self, text, **kwargs):
        # Tokenize the text using the provided tokenizer
        tokens = self._tokenizer(
            text, truncation=True, padding="max_length", return_tensors="pt"
        )
        return tokens

    def preprocess_text(self, text):
        # Perform all preprocessing steps
        tokens = self.tokenize(text[self._config.review_col])
        return tokens

    def augment_labeled_data(
        self, examples: pd.DataFrame, predictions: list
    ) -> pd.DataFrame:
        """Adds the examples and predictions to the labeled dataset."""
        predictions = pd.DataFrame(predictions)
        examples = pd.concat([examples, predictions], axis=1)
        self._labeled_data = pd.concat(
            [self._labeled_data, examples], axis=0
        ).reset_index(drop=True)
        self._logger.debug(
            f"Augmented {len(examples)} observations for a total labeled observations of {len(self._labeled_data)}"
        )
        self._logger.debug(f"{examples.head()}")

    def create_dataset(self, examples: pd.DataFrame) -> Dataset:
        """Creates a HuggningFace Dataset"""
        return Dataset.from_pandas(examples)

    def create_tokenized_datasets(self) -> DatasetDict:
        """Creates the HuggingFace training and test DatasetDict"""

        # Convert the labeled data from pandas DataFrame to Dataset.
        labeled_dataset = Dataset.from_pandas(
            self._labeled_data[[self._config.review_col, "label"]]
        )
        # Convert the labels to numerics
        label2id = {"NEG": 0, "NEU": 1, "POS": 2}
        labeled_dataset = labeled_dataset.map(
            lambda example: {"label": label2id[example["label"]]}
        )

        # Split the data into training/validation/test splits
        labeled_train_test_val = labeled_dataset.train_test_split(
            train_size=self._config.train_size, seed=self._config.random_state
        )
        labeled_test_val = labeled_train_test_val["test"].train_test_split(
            test_size=0.5, seed=self._config.random_state
        )

        labeled_data_splits = DatasetDict(
            {
                "train": labeled_train_test_val["train"],
                "validation": labeled_test_val["train"],
                "test": labeled_test_val["test"],
            }
        )

        # Tokenize the datasets and prepare them for training.
        tokenized_datasets = labeled_data_splits.map(self.preprocess_text, batched=True)
        tokenized_datasets = tokenized_datasets.rename_column("label", "labels")
        tokenized_datasets = tokenized_datasets.remove_columns(["content"])
        tokenized_datasets.set_format(
            type="torch",
            columns=["input_ids", "token_type_ids", "attention_mask", "labels"],
        )

        # Announce Tokenization
        train = len(tokenized_datasets["train"])
        val = len(tokenized_datasets["validation"])
        test = len(tokenized_datasets["test"])
        d = {"Train": train, "Validation": val, "Test": test}
        title = "Tokenized Dataset Sizes"
        self._logger.info(f"{print_dict(title=title, data=d)}")
        return tokenized_datasets

    def get_tokenized_batch(
        self, examples: pd.DataFrame, predictions: list
    ) -> DatasetDict:
        """Takes examples and predictions and returns a labeled HuggingFace DatasetDict"""
        self.augment_labeled_data(examples=examples, predictions=predictions)
        return self.create_tokenized_datasets()
