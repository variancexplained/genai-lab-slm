#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/tqa/task.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 11:53:03 am                                                #
# Modified   : Saturday February 8th 2025 11:54:39 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Text Quality Analysis Task Module"""
from __future__ import annotations

import multiprocessing
from typing import Any, Dict

import dask.dataframe as dd
import numpy as np
import pandas as pd
import spacy
from dask.distributed import Client, LocalCluster, progress
from spacy.matcher import Matcher

from genailab.core.dstruct import NestedNamespace
from genailab.flow.base.task import Task
from genailab.infra.utils.data.partition import DaskPartitioner

# ------------------------------------------------------------------------------------------------ #
#                                  DATASET SCHEMA                                                  #
# ------------------------------------------------------------------------------------------------ #
COUNT_SCHEMA = {
    "noun_count": np.float64,
    "verb_count": np.float64,
    "adjective_count": np.float64,
    "adverb_count": np.float64,
    "aspect_verb_pairs": np.float64,
    "noun_adjective_pairs": np.float64,
    "noun_phrases": np.float64,
    "verb_phrases": np.float64,
    "adverbial_phrases": np.float64,
    "review_length": 'int64',
    "lexical_density": np.float64,
    "dependency_depth": 'int64',
    "tqa_score": np.float64,
}

RATING_SCHEMA ={"tqa_rating": "int64"}

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
FINAL_DATASET_SCHEMA = {
    **DATASET_SCHEMA,
    **RATING_SCHEMA
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
    def __init__(self,
                 partitioner: DaskPartitioner,
                 coefficients: Dict[str, float],
                 schema: Dict[str, Any] = DATASET_SCHEMA,
                 normalized: bool = True,
                 batched: bool = True,
                 dask_config: bool = NestedNamespace,
                 spacy_config: bool = NestedNamespace,
                 **kwargs
                 ) -> None:
        super().__init__()
        # Task core config
        self._partitioner = partitioner
        self._coefficients = coefficients
        self._normalized = normalized
        self._batched = batched
        self._schema_dict = schema
        self._schema_df = pd.DataFrame(columns=schema.keys()).astype(schema)
        self._schema_tuple = tuple(self._schema_df.dtypes.to_dict().items())
        self._final_schema_df = pd.DataFrame(columns=FINAL_DATASET_SCHEMA.keys()).astype(FINAL_DATASET_SCHEMA)

        # Dask and Spacy Configurations
        self._dask_config = dask_config
        self._spacy_config = spacy_config

        # Spacy Config
        self._batch_size = spacy_config.batch_size
        self._n_process = spacy_config.n_process
        self._processes = spacy_config.processes

        # Dask Config
        self._nworkers = dask_config.nworkers
        self._memory_limit = dask_config.memory_limit
        self._threads_per_worker = dask_config.threads_per_worker

        # Progress bar
        self._pbar = None

        # Spacy NLP object and function words
        self._nlp = spacy.load("en_core_web_sm", enable=["tagger", "parser", "attribute_ruler"])
        self._function_words = self._nlp.Defaults.stop_words

        # Create Spacy Matchers for noun adjective and aspect verb pairs
        self._matcher = Matcher(self._nlp.vocab)
        noun_adjective_pattern = [{"POS": "NOUN"}, {"POS": "ADJ", "DEP": "amod"}]
        aspect_verb_pattern = [
            {"POS": "NOUN", "DEP": {"IN": ["nsubj", "dobj"]}},
            {"POS": "VERB"}
        ]

        self._matcher.add("NounAdjective", [noun_adjective_pattern])
        self._matcher.add("AspectVerb", [aspect_verb_pattern])


    def __hash__(self):
        """
        Implements a deterministic hash method for the TQATask object.
        This ensures that the object can be hashed deterministically by Dask.

        Returns:
            int: The hash value of the TQATask object.
        """
        return hash((tuple(self._coefficients.items()), self._normalized, self._nworkers, self._memory_limit, self._threads_per_worker, self._processes))



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
        """Analyzes text data using spaCy and Dask, performing feature extraction.

        This method initializes a Dask cluster, loads spaCy, and applies NLP processing
        to the provided data. It supports both batched and row-wise processing.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data.

        Returns:
            dd.DataFrame: Processed results as a Dask DataFrame.

        Raises:
            Exception: If an error occurs during analysis.
        """
        multiprocessing.set_start_method('spawn', force=True)

        cluster = LocalCluster(
            n_workers=self._nworkers,
            threads_per_worker=self._threads_per_worker,
            memory_limit=self._memory_limit,
            processes=self._processes,
            dashboard_address=":8787"
        )
        client = Client(cluster)

        try:
            # Convert pandas DataFrame to an optimally partitioned Dask DataFrame
            ddf = self.to_dash(pdf=data)

            if self._batched:

                results = ddf.map_partitions(
                    lambda batch: self._process_batch(batch),
                    meta=self._schema_df
                )
            else:
                results = ddf.apply(self._process_row, axis=1)

            # Assign task_rating to dataframe
            results = self._assign_tqa_rating(ddf=results)

            # Start computation in the background
            results = results.persist()

            # Display progress bar
            progress(results)

            # Compute and convert back to pandas
            pdf = self.to_pandas(ddf=results)


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

    def _process_batch(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        texts = batch_df['content'].tolist()  # Extract texts into a list

        results = []
        for i, doc in enumerate(self._nlp.pipe(texts, batch_size=self._batch_size, n_process=self._n_process)):
            original_row = batch_df.iloc[i].to_dict()
            if doc:  # Handle potential None docs (e.g., empty strings)
                result = self._process_doc(doc) # Process each doc
                combined_result = {**original_row, **result} # Merge dictionaries
                results.append(combined_result)
            else:
                # Handle empty reviews
                empty_row = {key: None for key in self._schema_df.columns}
                results.append(empty_row)


        batch_result = pd.DataFrame(results, columns=self._schema_df.columns)
        return batch_result

    def _process_doc(self, doc: spacy.tokens.Doc) -> Dict[str, float]:
        counts = {key: 0 for key in COUNT_SCHEMA.keys() if key != "tqa_score"}
        n_tokens = 0
        # Efficient Noun-Adjective and Aspect-Verb Pair Counting using spaCy's matcher
        matches = self._matcher(doc)

        for match_id, start, end in matches:
            if self._nlp.vocab.strings[match_id] == "NounAdjective":
                counts["noun_adjective_pairs"] += 1
            elif self._nlp.vocab.strings[match_id] == "AspectVerb":
                counts["aspect_verb_pairs"] += 1

        for token in doc:  # Iterate through the doc once
            n_tokens += 1
            if token.pos_ == "NOUN":
                counts["noun_count"] += 1
                if len(list(token.subtree)) > 1:
                    counts["noun_phrases"] += 1
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
        counts['review_length'] = n_tokens
        content_words = [word for word in doc if word not in self._function_words]
        counts['lexical_density'] = (len(content_words) / n_tokens) * 100 if n_tokens > 0 else 0

        # Compute dependency depth
        max_depth = 0
        for sent in doc.sents:
            depth = max([len(list(token.subtree)) for token in sent])
            max_depth = max(max_depth, depth)
        counts["dependency_depth"] = max_depth
        if self._normalized:
            for key in counts:
                counts[key] = self._log_normalize(counts[key])

        counts["tqa_score"] = sum(self._coefficients[key] * counts[key] for key in self._coefficients)
        return counts

    def _process_row(self, row: pd.Series) -> Dict[str, float]:
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
        n_tokens = 0

        # Create a Spacy Document from the review text
        doc = self._nlp(text=review)

        # Efficient Noun-Adjective and Aspect-Verb Pair Counting using spaCy's matcher
        matches = self._matcher(doc)

        # Handle empty review edge case
        if not review.strip():  # If review is empty or contains only whitespace
            return {**row, **counts}

        for match_id, start, end in matches:
            if self._nlp.vocab.strings[match_id] == "NounAdjective":
                counts["noun_adjective_pairs"] += 1
            elif self._nlp.vocab.strings[match_id] == "AspectVerb":
                counts["aspect_verb_pairs"] += 1

        for token in doc:
            n_tokens += 1
            if token.pos_ == "NOUN":
                counts["noun_count"] += 1
                if len(list(token.subtree)) > 1:
                    counts["noun_phrases"] += 1
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
        counts['review_length'] = n_tokens
        content_words = [word for word in doc if word not in self._function_words]
        counts['lexical_density'] = (len(content_words) / n_tokens) * 100 if n_tokens > 0 else 0

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

    def to_dash(self, pdf: pd.DataFrame) -> dd.DataFrame:
        """Converts a Pandas DataFrame to a Dask DataFrame.

        Args:
            pdf (pd.DataFrame): Input Pandas DataFrame.

        Returns:
            dd.DataFrame: Dask DataFrame partitioned according to the class settings.
        """
        data_size = pdf.memory_usage().sum()
        npartitions = self._partitioner.optimal_partitions(data_size=data_size)
        return dd.from_pandas(pdf, npartitions=npartitions)

    def to_pandas(self, ddf: dd.DataFrame) -> pd.DataFrame:
        """Converts a Dask DataFrame back to a Pandas DataFrame.

        Args:
            ddf (dd.DataFrame): Input Dask DataFrame.

        Returns:
            pd.DataFrame: Converted Pandas DataFrame.
        """
        if isinstance(ddf, dd.DataFrame):
            return pd.DataFrame(ddf, columns=self._final_schema_df.columns)
        else:
            return ddf.apply(pd.Series)
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

    def _assign_tqa_rating(self, ddf: dd.DataFrame, tqa_score_col="tqa_score"):
        """Assigns a TQA rating (1-5) based on percentiles of the TQA score in a Dask DataFrame.

        Args:
            ddf: Dask DataFrame with a TQA score column.
            tqa_score_col: The name of the TQA score column (default: "tqa_score").

        Returns:
            Dask DataFrame with the added "tqa_rating" column.
        """

        # 1. Compute Percentiles (using Dask)
        tqa_scores = ddf[tqa_score_col].compute()  # Compute the column to get numpy array
        percentiles = np.percentile(tqa_scores, [20, 40, 60, 80])

        # 2. Assign Ratings (using map_partitions for efficiency)
        def assign_ratings_partition(partition, percentiles):
            """Assigns ratings within a single partition"""
            def assign_rating(score):
                if score < percentiles[0]: return 1
                elif score < percentiles[1]: return 2
                elif score < percentiles[2]: return 3
                elif score < percentiles[3]: return 4
                else: return 5

            partition["tqa_rating"] = partition[tqa_score_col].apply(assign_rating)
            return partition

        ddf = ddf.map_partitions(lambda partition: assign_ratings_partition(partition=partition, percentiles=percentiles), meta=self._final_schema_df)

        return ddf