#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/perplexity/task.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Wednesday November 20th 2024 07:41:24 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import warnings

import pandas as pd
import torch
from tqdm import tqdm
from transformers import GPT2LMHeadModel, GPT2TokenizerFast

from discover.flow.data_prep.base.task import DataEnhancerTask
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.file.io import IOService

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
tqdm.pandas()


# ------------------------------------------------------------------------------------------------ #
class PerplexityAnalysisTask(DataEnhancerTask):
    """Task for performing perplexity analysis on text data.

    This class uses a pre-trained language model to calculate the perplexity
    of text data in a specified column. The results are added to a new column
    in the DataFrame, providing a quantitative measure of text coherence and
    complexity.

    Attributes:
        cache_filepath (str): Path to file containing perplexities computed in the cloud.
        device_local (bool): Whether to run locally, if cache isn't available. Default is False.
            If cache is not available, an exceptoin will be raised.
        column (str): The name of the column containing the text data. Defaults to "content".
        new_column (str): The name of the column to store perplexity scores. Defaults to "perplexity".
        model_name (str): The name of the pre-trained language model. Defaults to "distilbert/distilgpt2".
        stride (int): The stride size used for processing long sequences in chunks. Defaults to 512.
    """

    def __init__(
        self,
        cache_filepath: str,
        column="content",
        new_column="perplexity",
        model_name: str = "distilbert/distilgpt2",
        stride: int = 512,
        device_local: bool = False,
        io_cls: type[IOService] = IOService,
        **kwargs,
    ):
        super().__init__(new_column=new_column, **kwargs)
        self._model_name = model_name
        self._cache_filepath = cache_filepath
        self._device_local = device_local

        self._io = io_cls()

        self._model_name = model_name
        self._stride = stride

        # Model, tokenizer, and device are initialized as None and will be loaded later
        self._model = None
        self._tokenizer = None
        self._device = None
        self._max_length = None

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes perplexity on the given DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing text data.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing perplexity.
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
                msg = f"Cache not found or not available. {self.__class__.__name__} is not supported on local devices. Try running on Kaggle, Colab or AWS."
                self._logger.error(msg)
                raise FileNotFoundError(msg)
        except Exception as e:
            msg = f"Unknown exception encountered.\n{e}"
            self._logger.exception(msg)
            raise

    def _run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes perplexity analysis on the given DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing text data.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing perplexity scores.
        """
        # Clear CUDA memory to ensure enough space is available for the model
        torch.cuda.empty_cache()

        # Load the device, model, and tokenizer
        self._load_model_tokenizer_to_device()

        # Compute perplexity for each text entry in the specified column
        data[self._new_column] = data[self._column].progress_apply(
            self.predict_perplexity
        )
        # Write results to cache
        self._write_file(
            filepath=self._cache_filepath, data=data["id", self._new_column]
        )

        return data

    def predict_perplexity(self, text):
        """Calculates the perplexity of a given text using the loaded language model.

        Args:
            text (str): The input text for perplexity computation.

        Returns:
            float: The calculated perplexity score for the text.
        """
        with torch.no_grad():
            # Tokenize the text and prepare it for the model
            inputs = self._tokenizer(
                text.lower(),
                return_tensors="pt",
                truncation=True,
                padding=True,
                max_length=self._max_length,
            )
            # Move inputs to the appropriate device (CPU or GPU)
            inputs = {key: value.to(self._device) for key, value in inputs.items()}
            seq_len = inputs["input_ids"].size(1)
            nlls = []  # List to store negative log-likelihood values
            prev_end_loc = 0

            # Process the text in chunks using the specified stride
            for begin_loc in range(0, seq_len, self._stride):
                end_loc = min(begin_loc + self._max_length, seq_len)
                trg_len = end_loc - prev_end_loc  # Target length for the current chunk
                input_ids = inputs["input_ids"][:, begin_loc:end_loc].to(self._device)
                target_ids = input_ids.clone()
                target_ids[:, :-trg_len] = -100  # Mask non-target tokens

                with torch.no_grad():
                    # Compute the negative log-likelihood for the current chunk
                    outputs = self._model(input_ids, labels=target_ids)
                    neg_log_likelihood = outputs.loss

                nlls.append(neg_log_likelihood)
                prev_end_loc = end_loc
                if end_loc == seq_len:
                    break

        # Return the exponential of the average negative log-likelihood as perplexity
        return torch.exp(torch.stack(nlls).mean()).item()

    def _load_model_tokenizer_to_device(self) -> None:
        """Loads the device, tokenizer, and model for perplexity analysis."""
        # Select GPU if available, otherwise use CPU
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Load the tokenizer and model from the pre-trained model name
        self._tokenizer = GPT2TokenizerFast.from_pretrained(self._model_name)
        self._model = GPT2LMHeadModel.from_pretrained(self._model_name).to(self._device)

        # Set the maximum length supported by the model
        self._max_length = self._model.config.n_positions
