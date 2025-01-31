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
# Modified   : Friday January 31st 2025 03:54:11 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Syntactic Text Quality Analysis Task Module"""
import inspect
import logging
import multiprocessing
from abc import abstractmethod
from typing import Dict, Set

import dask.dataframe as dd
import numpy as np
import pandas as pd
import spacy
from dask.distributed import Client, LocalCluster
from tqdm import tqdm
from tqdm.dask import TqdmCallback

from genailab.flow.base.task import Task
from genailab.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
# Set the start method to 'fork' (Linux/Mac) or 'spawn' (Windows)
multiprocessing.set_start_method('spawn', force=True)
# ------------------------------------------------------------------------------------------------ #
dask_config = AppConfigReader().get_config(section="dask", namespace=False)
# ------------------------------------------------------------------------------------------------ #
cluster = LocalCluster(n_workers=dask_config["n_workers"],
                    threads_per_worker=dask_config["threads_per_worker"],
                    memory_limit=dask_config["memory_limit"],
                    processes=False,
                    )
client = Client(cluster)
# ------------------------------------------------------------------------------------------------ #

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
    "author": str,
    "rating": int,
    "content": str,
    "vote_sum": int,
    "vote_count": int,
    "date": "datetime64[ms]",
    "category": "category",
 **COUNT_SCHEMA,

}

# ------------------------------------------------------------------------------------------------ #
#                                    TQA TASK                                                      #
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
    def __init__(self,  coefficients: Dict[str, float], normalized: bool = True, batched: bool = True) -> None:
        super().__init__()
        self._coefficients = coefficients
        self._normalized = normalized
        self._batched  = batched
        self._dataset_meta = pd.DataFrame(columns=DATASET_SCHEMA.keys()).astype(DATASET_SCHEMA)
        self._n_partitions = dask_config["n_partitions"]

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __hash__(self):
        """
        Implements a deterministic hash method for the TQATask object.
        This ensures that the object can be hashed deterministically by Dask.

        Returns:
            int: The hash value of the TQATask object.
        """
        return hash((tuple(self._coefficients.items()), self._normalized, self._n_partitions))

    def __eq__(self, other):
        """
        Compares two TQATask objects for equality.

        Args:
            other (TQATask): Another TQATask object to compare.

        Returns:
            bool: True if both TQATask objects are equal, False otherwise.
        """
        if not isinstance(other, TQATask):
            return False
        return (self._coefficients == other._coefficients and
                self._normalized == other._normalized)

    def __getstate__(self):
        """
        Serializes the TQATask object for use with Dask.

        Returns:
            dict: A dictionary of the TQATask object's state.
        """
        state = self.__dict__.copy()
        return state

    def __setstate__(self, state):
        """
        Deserializes the TQATask object for use with Dask.

        Args:
            state (dict): A dictionary containing the serialized state of the TQATask object.
        """
        self.__dict__.update(state)

    @abstractmethod
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a batch of reviews and computes syntactic features and TQA syntactic scores.

        This method uses Dask's delayed computation to process the input DataFrame in parallel. It applies the
        `_process_review_with_metadata` method to each review and returns the results after computation.

        Args:
            data (pd.DataFrame): A Pandas DataFrame containing the reviews to process.

        Returns:
            pd.DataFrame: A DataFrame with the computed syntactic features and TQA score for each review.
        """
        pass

    def _log_normalize(self, count: int) -> float:
        """
        Applies log normalization (log(x + 1)) to a given count.

        This method is used to log-normalize the feature values to reduce the impact of extreme values and smooth the distribution.

        Args:
            count (int): The value to normalize.

        Returns:
            float: The log-normalized value.
        """
        return np.log1p(count)

    def _process_batch(self, batch_df: pd.DataFrame, function_words: Set[str], nlp: spacy.language.Language, meta: pd.DataFrame) -> pd.DataFrame:
        """
        Processes a batch of reviews using spaCy's `nlp.pipe()` for efficiency.

        This method applies SpaCy's `nlp.pipe()` to process a batch of reviews in parallel.
        It processes each review and computes the relevant syntactic features such as noun counts,
        verb counts, dependency depth, and lexical density. The results are returned as a Dask DataFrame.

        Args:
            batch_df (pd.DataFrame): A batch of reviews from the input DataFrame to process. Each row represents a review.
            function_words (Set[str]): A set of function words to exclude from lexical density calculations.
            nlp (spacy.language.Language): The spaCy language model used for processing the reviews.
            meta (pd.DataFrame): The schema (structure) of the dataset that defines the expected output columns.

        Returns:
            List[pd.DataFrame]: A list of one-row Pandas DataFrames.
        """
        self._logger.debug(f"Inside {self.__class__.__name__}: {inspect.currentframe().f_code.co_name}")

        # Process all reviews in the batch using spaCy's nlp.pipe(), with parallelization
        results = []

        # Process reviews using spaCy and create delayed results
        for i, row in batch_df.iterrows():
            result = self._process_row(row, function_words, nlp)
            results.append(result)

        return pd.DataFrame(results, columns=self._dataset_meta.columns)



    def _process_row(self, row: pd.Series, function_words: Set[str], nlp: spacy.language.Language) -> Dict[str, float]:
        """
        Processes a single review and computes its syntactic features, including
        counts of nouns, verbs, adjectives, adverbs, and dependency depth.

        Args:
            row (pd.Series): A single row of the input DataFrame, containing a review.
            function_words (Set[str]): Set of function words to exclude from lexical density.
            nlp (spacy.language.Language): The spaCy language model for processing the review.

        Returns:
            Dict[str, float]: A dictionary with the computed syntactic features and TQA score for the review.
        """
        review = row["content"]
        counts = {key: 0 for key in COUNT_SCHEMA.keys() if key != "tqa_score"}

        # Tokenize the review text into a spaCy doc
        doc = nlp(review)

        # Handle empty review edge case
        if not review.strip():  # If review is empty or contains only whitespace
            return {**row, **counts}

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

        # Compute review length, lexical density, and dependency depth
        counts['review_length'] = len(review.split())
        tokens = review.split()
        content_words = [word for word in tokens if word not in function_words]
        counts['lexical_density'] = (len(content_words) / len(tokens)) * 100 if len(tokens) > 0 else 0

        # Compute dependency depth
        max_depth = 0
        for sent in doc.sents:
            depth = max([len(list(token.subtree)) for token in sent])
            max_depth = max(max_depth, depth)
        counts["dependency_depth"] = max_depth

        # Log normalization if enabled
        if self._normalized:
            for key in counts:
                counts[key] = self._log_normalize(counts[key])

        counts["tqa_score"] = sum(self._coefficients[key] * counts[key] for key in self._coefficients)
        return {**row, **counts}


# ------------------------------------------------------------------------------------------------ #
#                                  TQ ANALYST DASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class TQAnalystDask:
    def __init__(self, schema: pd.DataFrame, npartitions: int, batched: bool = True) -> None:
        self._schema = schema
        self._npartitions = npartitions
        self._batched = batched

    def tqadask(self, data: dd.DataFrame) -> dd.DataFrame:
        try:
            # Initialize spaCy and Dask DataFrame
            nlp = spacy.load("en_core_web_sm")
            function_words = nlp.Defaults.stop_words
            ddf = dd.from_pandas(data, npartitions=self._npartitions)

            if self._batched:
                results = ddf.map_partitions(
                    lambda batch:
                    self._process_batch(
                        batch,
                        function_words=function_words,
                        nlp=nlp,
                        meta=self._dataset_meta
                    ),
                    meta=self._dataset_meta
                )
            else:
                results = ddf.apply(
                    lambda row: self._process_row(row, function_words, nlp),
                    axis=1,
                    meta=self._dataset_meta  # This defines the expected schema for each row's result
                )

            # Start computation in the background
            results = results.persist()

            with TqdmCallback():
            # Materialize results into a dask dataframe
                results = results.compute()


            # Convert to pandas
            if isinstance(results, dd.DataFrame):
                df = pd.DataFrame(results, columns=self._dataset_meta.columns)
            else:
                df = results.apply(pd.Series)

            return df
        finally:
            if client:
                client.close()
            if cluster:
                cluster.close()

# ------------------------------------------------------------------------------------------------ #
#                                 TQ ANALYST PANDAS                                                #
# ------------------------------------------------------------------------------------------------ #
class TQAnalystPandas:
    def __init__(self, schema: pd.DataFrame, npartitions: int, batched: bool = True) -> None:
        self._schema = schema
        self._npartitions = npartitions
        self._batched = batched

    def analyze(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Process a pandas DataFrame with TQDM progress bar and optional batching.

        Args:
            data (pd.DataFrame): The input DataFrame to process.
            schema (pd.DataFrame): The schema describing the structure of the output.
            npartitions (int): Number of partitions to simulate for processing.
            batched (bool): Whether to process data in batches. Defaults to True.

        Returns:
            pd.DataFrame: Processed DataFrame with the computed results.
        """
        try:
            # Initialize spaCy
            nlp = spacy.load("en_core_web_sm")
            function_words = nlp.Defaults.stop_words

            # Calculate the number of rows per partition
            partition_size = len(data) // self._npartitions
            results = []

            if self._batched:
                # Iterate over partitions with a progress bar
                for i in tqdm(range(self._npartitions), desc="Processing Partitions", unit="partition"):
                    start = i * partition_size
                    end = (i + 1) * partition_size if i != self._npartitions - 1 else len(data)
                    partition = data.iloc[start:end]
                    result_partition = self._process_batch(
                        partition,
                        function_words=function_words,
                        nlp=nlp,
                        meta=self._dataset_meta
                    )
                    results.append(result_partition)

                # Concatenate the results
                df = pd.concat(results, ignore_index=True)

            else:
                # Process data row-by-row with a progress bar
                df = data.apply(
                    lambda row: self._process_row(row, function_words, nlp),
                    axis=1,
                    meta=self._dataset_meta
                )
                df = df.apply(pd.Series)

            return df

        except Exception as e:
            logging.error(f"Error during processing: {e}")
            raise
