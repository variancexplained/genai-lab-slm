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
# Modified   : Sunday February 2nd 2025 12:13:31 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Task Module"""
from __future__ import annotations

import inspect
import logging
import multiprocessing
from abc import ABC
from typing import Any, Dict, Set

import dask.dataframe as dd
import numpy as np
import pandas as pd
import spacy
from dask.distributed import Client, LocalCluster
from pandarallel import pandarallel
from tqdm import tqdm

from genailab.flow.base.task import Task

pandarallel.initialize(progress_bar=True, nb_workers=18, verbose=0)
# ------------------------------------------------------------------------------------------------ #
#                                  DATASET SCHEMA                                                  #
# ------------------------------------------------------------------------------------------------ #
COUNT_SCHEMA = {
    "noun_count": np.float64,
    "verb_count": np.float64,
    "adjective_count": np.float64,
    "adverb_count": np.float64,
    "aspect_verb_pairs": np.float64,
    "noun_phrases": np.float64,
    "verb_phrases": np.float64,
    "adverbial_phrases": np.float64,
    "review_length": 'int64',
    "lexical_density": np.float64,
    "dependency_depth": 'int64',
    "tqa_score": np.float64,
}

DATASET_SCHEMA = {
    "id": 'str',
    "app_id": 'str',
    "app_name": 'str',
    "category_id": "category",
    "author": 'str',
    "rating": 'int64',
    "content": 'str',
    "vote_sum": 'int64',
    "vote_count": 'int64',
    "date": "datetime64[ms]",
    "category": "category",
 **COUNT_SCHEMA,

}

# ------------------------------------------------------------------------------------------------ #
#                                    TQA TASK                                                      #
# ------------------------------------------------------------------------------------------------ #
class TQATask(Task):
    """Class for handling the Text Quality Analysis (TQA) task with Dask or pandas processing.

    This class is responsible for executing the TQA task by using an analyst (either pandas or Dask-based)
    to process the data, applying coefficients for the TQA score calculation, and optionally normalizing
    the results. It supports batched processing for large datasets to enhance performance.

    Attributes:
        _coefficients (Dict[str, float]): A dictionary of coefficients for the TQA score calculation.
        _normalized (bool): Whether to normalize the TQA score. Defaults to True.
        _batched (bool): Whether to process data in batches. Defaults to True.
        _schema (pd.DataFrame): Metadata schema for the dataset.
        _npartitions (int): The number of partitions for processing data.
        _logger (logging.Logger): Logger instance for the task.

    Args:
        analyst (Analyst): The analyst object used for data processing (either pandas or Dask).
        coefficients (Dict[str, float]): The coefficients for TQA score calculation.
        normalized (bool, optional): Whether to normalize the TQA score. Defaults to True.
        batched (bool, optional): Whether to process data in batches. Defaults to True.
    """

    def __init__(self, analyst: Analyst) -> None:
        self._analyst = analyst


    def __hash__(self):
        """
        Implements a deterministic hash method for the TQATask object.
        This ensures that the object can be hashed deterministically by Dask.

        Returns:
            int: The hash value of the TQATask object.
        """
        return hash((tuple(self._coefficients.items()), self._normalized, self._npartitions))



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
        return self._analyst.analyze(data=data)




# ------------------------------------------------------------------------------------------------ #
#                                     ANALYST                                                      #
# ------------------------------------------------------------------------------------------------ #
class Analyst(ABC):

    def __init__(self, coefficients: Dict[str, float], normalized: bool = True, batched: bool = True) -> None:
        super().__init__()

        self._coefficients = coefficients
        self._normalized = normalized
        self._batched = batched


        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def __eq__(self, other):
        """
        Compares two TQATask objects for equality.

        Args:
            other (TQATask): Another TQATask object to compare.

        Returns:
            bool: True if both TQATask objects are equal, False otherwise.
        """
        if not isinstance(other, Analyst):
            return False
        return (self._coefficients == other._coefficients and
                self._normalized == other._normalized)

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

        # Process all reviews in the batch using spaCy's nlp.pipe(), with parallelization
        results = []

        try:

            # Process reviews using spaCy and create delayed results
            for i, row in batch_df.iterrows():
                result = self._process_row(row, function_words, nlp)
                results.append(result)

            batch_result = pd.DataFrame(results, columns=self._schema_df.columns)
            self._logger.debug(f"Batch result type, expected Pandas Dataframe. Returned: {type(batch_result)}")
            return batch_result
        except Exception as e:
            msg = f"Exception occurred.\n{e}"
            self._logger.exception(msg)
            raise



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
        self._logger.debug(f"Row should be a series. Actual type: {type(row)}\n{row}")
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
        row_result = pd.Series({**row.to_dict(), **counts})
        self._logger.debug(f"Row result should be a series. Actual type: {type(row_result)}\n{row_result}")
        return row_result

# ------------------------------------------------------------------------------------------------ #
#                                  TQ ANALYST DASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class TQAnalystDask(Analyst):

    def __init__(self,
                 schema: Dict[str,Any],
                 coefficients: Dict[str,float],
                 npartitions: int = 72,
                 normalized: bool = True,
                 batched: bool = True,
                 nworkers: int = 6,
                 memory_limit: str = "11GiB",
                 threads_per_worker: int = 1,
                 processes: bool = False,
                 **kwargs
                 ) -> None:
        super().__init__(coefficients=coefficients, normalized=normalized, batched=batched)

        self._schema_dict = schema
        self._schema_df = pd.DataFrame(columns=schema.keys()).astype(schema)
        self._schema_tuple = tuple(self._schema_df.dtypes.to_dict().items())
        self._logger.debug(f"Type of schema_dict: {type(self._schema_dict)}\n{self._schema_dict}")
        self._logger.debug(f"Type of schema_df: {type(self._schema_df)}\n{self._schema_df}")
        self._logger.debug(f"Type of schema_tuple: {type(self._schema_tuple)}\n{self._schema_tuple}")

        self._npartitions = npartitions
        self._nworkers = nworkers
        self._memory_limit = memory_limit
        self._threads_per_worker = threads_per_worker
        self._processes = processes

    def analyze(self, data: pd.DataFrame) -> dd.DataFrame:
        self._logger.debug(f"Inside {self.__class__.__name__}: {inspect.currentframe().f_code.co_name}")


        # ---------------------------------------------------------------------------------------- #
        multiprocessing.set_start_method('spawn', force=True)

        cluster = LocalCluster(n_workers=self._nworkers,
                            threads_per_worker=self._threads_per_worker,
                            memory_limit=self._memory_limit,
                            processes=self._processes,
                            dashboard_address=":8787"
                            )
        client = Client(cluster)
        # ---------------------------------------------------------------------------------------- #
        try:
            # Initialize spaCy and Dask DataFrame
            nlp = spacy.load("en_core_web_sm")
            function_words = nlp.Defaults.stop_words

            # Sample Data
            # data = data.sample(n=50)

            # Convert pandas dataframe to dask
            ddf = self.to_dash(pdf=data)
            self._logger.debug(f"Converted pandas to {type(ddf)}..")

            if self._batched:
                self._logger.debug("Processing batches..")
                results = ddf.map_partitions(
                    lambda batch:
                    self._process_batch(
                        batch,
                        function_words=function_words,
                        nlp=nlp,
                        meta=self._schema_df
                    ),
                    meta=self._schema_df
                )
            else:
                self._logger.debug("Processing row at a time..")
                results = ddf.apply(self._process_row, axis=1, meta=self._schema_df, args=(function_words, nlp))
                self._logger.debug(f"Result type: {type(results)}")

            # Start computation in the background
            results = results.persist()
            self._logger.debug(f"Persisted results of type {type(results)}.")

            # Materialize results into a dask dataframe
            results = results.compute()
            self._logger.debug(f"Computed results of type {type(results)}.")
            self._logger.debug(f"Result type: {results}")

            # Convert dask dataframe back to pandas
            pdf = self.to_pandas(ddf=results)
            self._logger.debug(f"Converted dask to {type(pdf)}.")

            return pdf
        except Exception as e:
            msg = f"Exception occurred.\n{e}"
            self._logger.exception(msg)
            raise
        finally:
            if client:
                client.close()
            if cluster:
                cluster.close()


    def to_dash(self, pdf: pd.DataFrame) -> dd.DataFrame:
        self._logger.debug(f"Inside {self.__class__.__name__}: {inspect.currentframe().f_code.co_name}")
        return dd.from_pandas(pdf, npartitions=self._npartitions)


    def to_pandas(self, ddf: dd.DataFrame) -> pd.DataFrame:
        self._logger.debug(f"Inside {self.__class__.__name__}: {inspect.currentframe().f_code.co_name}")
        if isinstance(ddf, dd.DataFrame):
            return pd.DataFrame(ddf, columns=self._schema_df.columns)
        else:
            return ddf.apply(pd.Series)


# ------------------------------------------------------------------------------------------------ #
#                                 TQ ANALYST PANDAS                                                #
# ------------------------------------------------------------------------------------------------ #
class TQAnalystPandas(Analyst):
    def __init__(self,
                 coefficients: Dict[str,float],
                 normalized: bool = True,
                 npartitions: int = 72,
                 batched: bool = True,
                 **kwargs
                 ) -> None:
        super().__init__(coefficients=coefficients, normalized=normalized, batched=batched)
        self._npartitions = npartitions

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
                        meta=self._schema_df
                    )
                    results.append(result_partition)

                # Concatenate the results
                df = pd.concat(results, ignore_index=True)

            else:
                # Process data row-by-row with a progress bar
                df = data.apply(
                    lambda row: self._process_row(row, function_words, nlp)
                )
                df = df.parallel_apply(pd.Series)

            return df

        except Exception as e:
            logging.error(f"Error during processing: {e}")
            raise
