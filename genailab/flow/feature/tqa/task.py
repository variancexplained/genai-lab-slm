#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/feature/tqa/task.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:53:03 am                                                #
# Modified   : Thursday January 30th 2025 12:05:24 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Task Module"""
import multiprocessing
from typing import Dict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import spacy
from dask.diagnostics import ProgressBar
from dask.distributed import Client, LocalCluster

from genailab.flow.base.task import Task
from genailab.infra.config.app import AppConfigReader
from genailab.infra.service.logging.task import task_logger
from genailab.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
# Set the start method to 'fork' (Linux/Mac) or 'spawn' (Windows)
multiprocessing.set_start_method('fork', force=True)
# ------------------------------------------------------------------------------------------------ #
dask_config = AppConfigReader().get_config(section="dask", namespace=False)
# ------------------------------------------------------------------------------------------------ #
cluster = LocalCluster(n_workers=dask_config["n_workers"],
                       threads_per_worker=dask_config["threads_per_worker"],
                       memory_limit=dask_config["memory_limit"])
client = Client(cluster)
# ------------------------------------------------------------------------------------------------ #
# Set the start method to 'fork' (Linux/Mac) or 'spawn' (Windows)
multiprocessing.set_start_method('fork', force=True)
# ------------------------------------------------------------------------------------------------ #
#                                  DATASET SCHEMA                                                  #
# ------------------------------------------------------------------------------------------------ #
COUNT_SCHEMA = {
    "noun_count": float,
    "verb_count": float,
    "adjective_count": float,
    "adverb_count": float,
    "aspect_verb_pairs": float,
    "noun_phrases": float,
    "verb_phrases": float,
    "adverbial_phrases": float,
    "review_length": int,
    "lexical_density": float,
    "dependency_depth": int,
    "tqa_score": float,
}

DATASET_SCHEMA = {
    "id": str,
    "app_id": str,
    "app_name": str,
    "category_id": "category",
    "category": "category",
    "author": str,
    "rating": int,
    "content": str,
    "vote_sum": int,
    "vote_count": int,
    "date": "datetime64[ms]",
 **COUNT_SCHEMA,

}

# ------------------------------------------------------------------------------------------------ #
#                                        TQA TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
class TQATask(Task):
    """
    Task for computing syntactic features and generating TQA syntactic scores for reviews.

    This class processes reviews to compute various syntactic features like noun counts,
    verb counts, adjective counts, dependency depth, and more. The features are then
    used to compute a TQA syntactic score, which is based on a set of coefficients.

    Attributes:
        _coefficients (dict): A dictionary of feature coefficients to compute the TQA score.
        _normalized (bool): Whether to apply log normalization to the computed features.
        _logger (logging.Logger): Logger instance for logging events.
    """
    def __init__(self, coefficients: Dict[str, float], normalized: bool = True) -> None:
        super().__init__()
        self._coefficients = coefficients
        self._normalized = normalized
        self._n_partitions = dask_config["n_partitions"]
        self._n_process = dask_config["n_process"]


    def __hash__(self):
        """
        Implementing a deterministic hash method for the TQATask object.
        This ensures that the object can be hashed deterministically by Dask.
        """
        return hash((tuple(self._coefficients.items()), self._normalized, self._n_partitions))

    def __eq__(self, other):
        """
        Ensures that two TQATask objects are compared correctly.
        """
        if not isinstance(other, TQATask):
            return False
        return (self._coefficients == other._coefficients and
                self._normalized == other._normalized)

    def __getstate__(self):
        """
        Control how the object is serialized.
        """
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        """
        Control how the object is deserialized.
        """
        self.__dict__.update(state)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a batch of reviews and computes syntactic features and TQA syntactic scores.

        This method uses Dask to process the input DataFrame in parallel. It applies the
        `_process_batch` method to each partition of the data and returns the results after
        computation.

        Args:
            data (pd.DataFrame): A Pandas DataFrame containing the reviews to process.

        Returns:
            pd.DataFrame: A DataFrame with the computed syntactic features and TQA score for each review.
        """
        Printer().print_dict(title=f"{self.__class__.__name__} Configuration", data=dask_config)
        nlp = spacy.load("en_core_web_sm")
        function_words = nlp.Defaults.stop_words
        ddf = dd.from_pandas(data, npartitions=self._n_partitions)

        with ProgressBar():
            results = ddf.map_partitions(self._process_batch, function_words=function_words, nlp=nlp, meta=DATASET_SCHEMA).persist()

        return results.compute()

    def _log_normalize(self, count: int) -> float:
        """
        Applies log normalization (log(x + 1)) to a given count.

        This method is used to log-normalize the feature values to reduce the impact of
        extreme values and smooth the distribution.

        Args:
            count (int): The value to normalize.

        Returns:
            float: The log-normalized value.
        """
        return np.log1p(count)

    def _process_batch(self, batch_df, function_words, nlp):
        """
        Processes a batch of reviews using spaCy's `nlp.pipe()` for efficiency.

        This method applies SpaCy's `nlp.pipe()` to process a batch of reviews in parallel.
        It processes each review and computes the relevant syntactic features using the
        `_process_review_with_metadata` method.

        Args:
            batch_df (pd.DataFrame): A batch of reviews from the input DataFrame to process.

        Returns:
            pd.DataFrame: A DataFrame with computed syntactic features for each review in the batch.
        """
        docs = list(nlp.pipe(batch_df["content"]))
        results = []
        for i, doc in enumerate(docs):
            row = batch_df.iloc[i]
            row_result = self._process_review_with_metadata(row, function_words, nlp)
            results.append(row_result)
        return pd.DataFrame(results)

    def _process_review_with_metadata(self, row, function_words, nlp):
        """
        Processes a single review and computes its syntactic features, including
        counts of nouns, verbs, adjectives, adverbs, and dependency depth.

        This method extracts various syntactic features from a review, computes lexical
        density, review length, dependency depth, and applies log normalization if enabled.
        The resulting features are then used to compute the TQA syntactic score.

        Args:
            row (pd.Series): A single row of the input DataFrame, containing a review.

        Returns:
            pd.Series: A Pandas Series with the computed syntactic features and TQA score
                        for the review.
        """
        review = row["content"]
        counts = {key: 0 for key in COUNT_SCHEMA.keys() if key != "tqa_score"}

        # Handle empty review edge case
        if not review.strip():  # If review is empty or contains only whitespace
            return pd.Series({**row, **counts})

        doc = nlp(review)
        for token in doc:
            if token.pos_ == "NOUN":
                counts["noun_count"] += 1
                if len(list(token.subtree)) > 1:
                    counts["noun_phrases"] += 1
                if token.dep_ in ("nsubj", "dobj") and token.head.pos_ == "VERB":
                    counts["aspect_verb_pairs"] += 1
            elif token.pos_ == "VERB":
                counts["verb_count"] += 1
                if len(list(token.subtree)) > 1:
                    counts["verb_phrases"] += 1
            elif token.pos_ == "ADJ":
                counts["adjective_count"] += 1
            elif token.pos_ == "ADV":
                counts["adverb_count"] += 1
                if len(list(token.subtree)) > 1:
                    counts["adverbial_phrases"] += 1

        # Compute review length (total number of words)
        counts['review_length'] = len(review.split())

        # Compute lexical density (content words / total words) scaled by 100
        tokens = review.split()
        content_words = [word for word in tokens if word not in function_words]
        counts['lexical_density'] = (len(content_words) / len(tokens)) * 100 if len(tokens) > 0 else 0

        # Compute dependency depth
        max_depth = 0
        for sent in doc.sents:  # Process each sentence in the document
            # For each sentence, compute the depth of the tree
            depth = max([len(list(token.subtree)) for token in sent])
            max_depth = max(max_depth, depth)
        counts["dependency_depth"] = max_depth

        # Log normalization if enabled
        if self._normalized:
            for key in counts:
                counts[key] = self._log_normalize(counts[key])

        counts["tqa_score"] = sum(self._coefficients[key] * counts[key] for key in self._coefficients)
        return pd.Series({**row, **counts})



