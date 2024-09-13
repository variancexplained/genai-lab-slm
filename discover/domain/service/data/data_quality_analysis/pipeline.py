#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/data_quality_analysis/pipeline.py                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Friday September 13th 2024 12:01:28 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Module"""
from __future__ import annotations

import os
import re
import warnings
from dataclasses import dataclass
from typing import AnyStr, List, Optional, Union

import emoji
import fasttext
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from pandarallel import pandarallel
from profanity_check import predict
from tqdm import tqdm

from discover.application.pipeline import Pipeline, PipelineBuilder, StageConfig
from discover.data_prep import log_exceptions, task_profiler
from discover.data_prep.io import ReadTask, WriteTask
from discover.domain.service.base.task import Task
from discover.shared.persist.object.cache import Cache, CacheIterator
from discover.utils.base import Reader, Writer
from discover.utils.data import split_dataframe
from discover.utils.io import PandasReader, PandasWriter
from discover.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
fasttext.FastText.eprint = lambda x: None


# ------------------------------------------------------------------------------------------------ #
#                                 DATA QUALITY CONFIG                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQAConfig(StageConfig):
    """Class encapsulating the configuration for data quality assessment stage."""

    name: str = "DataQualityAssessment"
    source_directory: str = "01_stage/reviews"
    source_filename: str = None
    target_directory: str = "02_dqa/reviews"
    target_filename: str = None
    cache_name: str = "dqa"
    partition_cols: str = "category"
    n_jobs: int = 18
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
#                              DATA QUALITY ASSESSMENT                                             #
# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessment(PipelineBuilder):
    """Data quality assessment class.

    This class constructs and executes a pipeline to detect data quality issues
    in a dataset. If the endpoint already exists and config.force is False,
    the pipeline is not executed; instead, the endpoint is read and
    returned.

    Attributes:
        data (pd.DataFrame): The input data for quality analysis.

    Args:
        config (DQAConfig): Configuration for the data quality assessment stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
    """

    def __init__(
        self,
        config: DQAConfig,
        source_reader_cls: type[Reader] = PandasReader,
        target_writer_cls: type[Writer] = PandasWriter,
        target_reader_cls: type[Reader] = PandasReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
        cache_cls: type[Cache] = Cache,
        fastmode: bool = False,
    ) -> None:
        """Initializes the DataQualityPipeline with data."""
        super().__init__(
            config=config,
            source_reader_cls=source_reader_cls,
            target_writer_cls=target_writer_cls,
            target_reader_cls=target_reader_cls,
            pipeline_cls=pipeline_cls,
            review_repo_cls=review_repo_cls,
        )
        self._cache = cache_cls(name=self.config.cache_name)
        self._pipeline = self.create_pipeline()

    def overview(self) -> pd.DataFrame:
        """Provides an overview of the data quality issues in terms of counts and percentages."""
        try:
            overview = (
                self._data.loc[:, self._data.columns.str.startswith("dqa_")]
                .sum()
                .to_frame()
            )
            overview.columns = ["Count"]
            overview["Percent"] = overview["Count"] / len(self._data) * 100
            overview = overview.style.format(thousands=",", precision=2)
            return overview
        except Exception as e:
            msg = f"Exception occurred in overview. \n{e}"
            self.logger.exception(msg)
            raise

    def create_pipeline(self) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = self.pipeline_cls(name=self.config.name)

        # Instantiate IO Tasks
        load = ReadTask(
            directory=self.config.source_directory,
            filename=self.config.source_filename,
            reader_cls=self.source_reader_cls,
        )
        save = WriteTask(
            directory=self.config.target_directory,
            filename=self.config.target_filename,
            writer_cls=self.target_writer_cls,
            partition_cols=self.config.partition_cols,
        )

        # Instantiate DQA initializer
        init = DQAInitTask(sort_by="id")

        # Instantiate DQA Finalizer
        finalizer = DQAFinalizeTask(cache_name=self.config.cache_name)

        # Instantiate duplicate checkers
        dup1 = DetectDuplicateRowTask(new_column_name="is_duplicate", cache=self._cache)
        dup2 = DetectDuplicateRowTask(
            column_names="id",
            new_column_name="is_duplicate_review_id",
            cache=self._cache,
        )
        # Instantiate detection of null values
        null = DetectNullValuesTask(
            new_column_name="has_null_values", cache=self._cache
        )

        # Instantiate detection of outliers
        out_vote_sum = DetectOutliersTask(
            column_name="vote_sum",
            new_column_name="vote_sum_outlier",
            cache=self._cache,
        )
        out_vote_count = DetectOutliersTask(
            column_name="vote_count",
            new_column_name="vote_count_outlier",
            cache=self._cache,
        )
        # Instantiate detection of non-english text in reviews and app names.
        lang_review = DetectNonEnglishTask(
            text_column="content",
            new_column_name="has_non_english_review",
            n_jobs=self.config.n_jobs,
            cache=self._cache,
        )
        lang_app_name = DetectNonEnglishTask(
            text_column="app_name",
            new_column_name="has_non_english_app_name",
            n_jobs=self.config.n_jobs,
            cache=self._cache,
        )
        # Instantiate detection of emojis
        emoji = DetectEmojiTask(new_column_name="has_emojis", cache=self._cache)
        # Instantiate detection of excessive special characters
        chars = DetectSpecialCharacterTask(
            threshold=0.3,
            new_column_name="has_excessive_special_chars",
            cache=self._cache,
        )
        # Instantiate date validator
        dates = DetectInvalidDatesTask(
            new_column_name="has_invalid_date", cache=self._cache
        )
        # Instantiate a rating validator
        ratings = DetectInvalidRatingsTask(
            new_column_name="has_invalid_rating", cache=self._cache
        )
        # Instantiate a profanity check
        profanity = DetectProfanityTask(
            n_jobs=self.config.n_jobs,
            new_column_name="has_profanity",
            cache=self._cache,
        )
        # Instantiate email, URL, and phone number detection in review text.
        emails = DetectEmailTask(new_column_name="contains_email", cache=self._cache)
        urls = DetectURLTask(new_column_name="contains_url", cache=self._cache)
        phones = DetectPhoneNumberTask(
            new_column_name="contains_phone_number", cache=self._cache
        )

        # Add tasks to pipeline, starting with the lower resource intensive tasks.
        pipe.add_task(load)
        pipe.add_task(init)
        pipe.add_task(dup1)
        pipe.add_task(dup2)
        pipe.add_task(null)
        pipe.add_task(out_vote_sum)
        pipe.add_task(out_vote_count)
        pipe.add_task(emoji)
        pipe.add_task(chars)
        pipe.add_task(dates)
        pipe.add_task(ratings)
        pipe.add_task(emails)
        pipe.add_task(urls)
        pipe.add_task(phones)
        # Add resource moderate tasks.
        pipe.add_task(lang_review)
        pipe.add_task(lang_app_name)
        # Add resource intensive task
        pipe.add_task(profanity)
        # Finalizer
        pipe.add_task(finalizer)
        pipe.add_task(save)
        return pipe


# ------------------------------------------------------------------------------------------------ #
#                                  DQA INIT TASK                                                   #
# ------------------------------------------------------------------------------------------------ #
class DQAInitTask(Task):
    """Initializes the data for the data quality assessment

    Args:
        sort_by (str): The column to sort by.
    """

    def __init__(self, sort_by: str = "id") -> None:
        super().__init__()
        self._sort_by = sort_by

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Performs the task

        Args:
            data (pd.DataFrame): Data to be assessed.
        """
        data = data.reset_index().set_index(keys=[self._sort_by])
        data = data.sort_index(ascending=True)
        return data.reset_index()


# ------------------------------------------------------------------------------------------------ #
#                                  DQA FINALIZE TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DQAFinalizeTask(Task):
    """Finalizes the data for the data quality assessment

    Args:
        cache_cls (type[Cache]): Cache manager class.
    """

    def __init__(
        self,
        cache_iter_cls: type[CacheIterator] = CacheIterator,
        cache_name: str = "dqa",
    ) -> None:
        super().__init__()
        self._cache_name = cache_name
        self._cache_iter_cls = cache_iter_cls

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Performs the task

        Args:
            data (pd.DataFrame): Data to be assessed.
        """
        cache_iter = self._cache_iter_cls(name=self._cache_name)
        for column in cache_iter:
            self._logger.debug(
                f"Obtaining {column.name} from cache with {len(column)} rows."
            )
            data = pd.concat([data, column], axis=1)
            self._logger.debug(f"Data now has {data.shape[1]} columns.")

        self._logger.debug(f"Final Data Head: \n{data.head()}")
        self._logger.debug(f"Final Data Tail: \n{data.tail()}")
        return data


# ------------------------------------------------------------------------------------------------ #
#                               DATA QUALITY ASSESSMENT TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentTask(Task):
    """Base class for data quality tasks.

    Args:
        new_column_name (str): The column name used as an indicator of the data quality element.
    """

    PREFIX = "dqa_"

    def __init__(
        self,
        new_column_name: str,
        cache: Cache,
    ):
        super().__init__()
        self._new_column_name = f"{self.PREFIX}{new_column_name}"
        self._cache = cache

    @property
    def new_column_name(self) -> str:
        """Returns the new column a data quality assessment subtask adds to the dataset."""
        return self._new_column_name

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the underlying subtask's run method if result not in cache.

        Args:
            data (pd.DataFrame): Data to be assessed.
        """
        self.start_task()
        # Execute the underlying task if the result does not exist in the cache.
        if not self._cache.exists(key=self.new_column_name):
            result = self.run(data=data)
            self._cache.add_item(key=self.new_column_name, value=result)

        else:
            self._logger.debug(
                f"Cache for {self.new_column_name} already exists. Skipping task."
            )
        self.stop_task()
        return data


# ------------------------------------------------------------------------------------------------ #
#                               DETECT DUPLICATE ROW TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateRowTask(DataQualityAssessmentTask):
    """A task to mark duplicate rows in a DataFrame.

    Attributes:
        column_names (list): A list of column names to consider when identifying duplicates.
        new_column_name (str): The name of the column to add, indicating whether each row is a duplicate.
    """

    def __init__(
        self,
        cache: Cache,
        column_names: Optional[Union[List[AnyStr], AnyStr]] = None,
        new_column_name: str = "is_duplicate",
    ):
        """
        Initialize the DetectDuplicateRowTask.

        Parameters:
            column_names (Optional[Union[List[AnyStr], AnyStr]]): Columns to consider  when identifying duplicates.
                Default is to use all columns.
            new_column_name (str): The name of the column to add, indicating whether each row is a duplicate.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._column_names = column_names

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Returns a binary series indicating the rows that contain duplicate values.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            result (pd.Series): A series of binary indicator values.
        """
        # Check for duplicates.
        result = data.duplicated(subset=self._column_names, keep="first")
        return result.rename(self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                                 DATA NULL VALUES TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(DataQualityAssessmentTask):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(
        self,
        cache: Cache,
        new_column_name: str = "has_null",
    ):
        super().__init__(cache=cache, new_column_name=new_column_name)

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """

        result = data.isnull().values.any(axis=1)
        return pd.Series(data=result, name=self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                            DETECT NON-ENGLISH TASK                                               #
# ------------------------------------------------------------------------------------------------ #
# Standalone function for processing a chunk
def process_chunk(chunk, text_column, new_column, model_path):
    model = fasttext.load_model(model_path)  # Load the model in each worker
    return chunk[text_column].apply(lambda text: is_non_english(text, model))


# Standalone function to determine if text is non-English
def is_non_english(text, model):
    try:
        predictions = model.predict(text)
        return predictions[0][0] != "__label__en"
    except Exception as e:
        # Use a simple print statement for error logging in parallel function
        print(f"Error in language detection: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #
class DetectNonEnglishTask(DataQualityAssessmentTask):
    """Detects which reviews are non-English.

    Args:
        text_column (str): Name of the column containing text to be analyzed.
        new_column_name (str): Name of the new column to be created indicating whether the text is in English or not.
    """

    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "non_english",
        n_jobs: int = 12,
    ):
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        # Split data into chunks
        chunks = split_dataframe(data=data, n=self._n_jobs)
        self._logger.debug(f"Data split into {len(chunks)} chunks")

        # Process chunks in parallel using joblib
        results = Parallel(n_jobs=self._n_jobs)(
            delayed(process_chunk)(
                chunk, self._text_column, self.new_column_name, self._model_filepath
            )
            for chunk in tqdm(chunks)
        )

        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        return result.rename(self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMOJI TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(DataQualityAssessmentTask):
    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "has_emoji",
    ):
        """
        Initializes the DetectEmojiTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._text_column].parallel_apply(self._contains_emoji)

        return result.rename(self.new_column_name)

    @staticmethod
    def _contains_emoji(text):
        """Detects if the given text contains any emojis.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains emojis, False otherwise.
        """
        return any(char in emoji.EMOJI_DATA for char in text)


# ------------------------------------------------------------------------------------------------ #
#                            DETECT SPECIAL CHARACTERS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectSpecialCharacterTask(DataQualityAssessmentTask):
    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "has_excessive_special_chars",
        threshold: float = 0.2,
    ):
        """Initializes the DetectSpecialCharacterTask with the column names.

        Class flags rows with excessive number of special characters in the review text. Excessive
        is defined in terms of a proportion of overall review length.
        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
            threshold (float): Proportion of review text with special characters, above which, is considered
                excessive
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column
        self._threshold = threshold

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._text_column].parallel_apply(
            self._contains_excessive_special_chars
        )

        return result.rename(self.new_column_name)

    def _contains_excessive_special_chars(self, text):
        """Detects if the given text contains an excessive number of special characters.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains excessive special characters, False otherwise.
        """
        special_chars = re.findall(r"[^\w\s]", text)
        return (
            len(special_chars) / len(text) > self._threshold if len(text) > 0 else False
        )


# ------------------------------------------------------------------------------------------------ #
#                               DETECT INVALID DATES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidDatesTask(DataQualityAssessmentTask):
    def __init__(self, cache: Cache, new_column_name: str = "date_invalid"):
        """
        Detects dates that are outside the range of 2008-2023

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid dates and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid dates.

        """

        result = data["date"].parallel_apply(self._date_invalid)

        return result.rename(self.new_column_name)

    def _date_invalid(self, date: np.datetime64) -> bool:
        """Indicates whether the rating is on the five point scale."""
        date = np.datetime64(date)

        return date < np.datetime64("2007") or date > np.datetime64("2024")


# ------------------------------------------------------------------------------------------------ #
#                               DETECT INVALID RATINGS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(DataQualityAssessmentTask):
    def __init__(self, cache: Cache, new_column_name: str = "rating_invalid"):
        """
        Detects ratings that are not on the five-point scale.

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid ratings and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid ratings.

        """

        result = data["rating"].parallel_apply(self._rating_invalid)

        return result.rename(self.new_column_name)

    def _rating_invalid(self, rating: int) -> bool:
        """Indicates whether the rating is on the five point scale."""
        return rating < 0 or rating > 5


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT PROFANITY                                                 #
# ------------------------------------------------------------------------------------------------ #
def detect_profanity(text):
    """
    Detects if the given text contains any profanity.

    Args:
        text (str): The text to be analyzed.

    Returns:
        bool: True if the text contains profanity, False otherwise.
    """
    try:
        return predict([text])[0] == 1
    except Exception as e:
        print(f"Error in profanity detection: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #
class DetectProfanityTask(DataQualityAssessmentTask):
    """Detects profanity in the designated column of a dataframe.

    Note: This class leverages the multiprocessing module. To avoid unintended behavior and
    recursively spawning processes, this should be executed with the __main__ shield. This
    is not strictly required in a WSL2 virtual machine, but should be refactored behind
    a __main__  shield for cross-platform portability.
    """

    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "has_profanity",
        n_jobs: int = 12,
    ):
        """
        Initializes the DetectProfanityTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating
                whether the text contains profanity or not.
            n_jobs (int): Number of cpus corresponding to 'jobs' in a concurrent context.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column
        self._n_jobs = n_jobs

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect profanity in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """
        chunks = split_dataframe(data=data, n=100)
        self._logger.debug(f"Data split into {len(chunks)} chunks")

        results = [
            chunk.parallel_apply(
                lambda row: detect_profanity(row[self._text_column]), axis=1
            )
            for chunk in tqdm(chunks)
        ]
        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        return result.rename(self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMAIL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(DataQualityAssessmentTask):
    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "contains_email",
    ):
        """Initializes the DetectSpecialPatternsTask with the column name containing text data.


        Args:
            text_column (str): Name of the column containing text to be analyzed.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """Detects special patterns (emails, URLs, phone numbers) in the specified text column
        and adds new columns indicating the presence of each pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_email)
        return result.rename(self.new_column_name)

    @staticmethod
    def _contains_email(text):
        """
        Detects if the given text contains an email address.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains an email address, False otherwise.
        """
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        return bool(re.search(email_pattern, text))


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT URL TASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class DetectURLTask(DataQualityAssessmentTask):
    """Detects the presence of URLs in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "contains_url",
    ):
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_url)
        return result.rename(self.new_column_name)

    @staticmethod
    def _contains_url(text):
        """
        Detects if the given text contains a URL.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains a URL, False otherwise.
        """
        url_pattern = r"\b(?:https?://|www\.)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?\b"
        return bool(re.search(url_pattern, text))


# ------------------------------------------------------------------------------------------------ #
#                               DETECT PHONE NUMBER TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectPhoneNumberTask(DataQualityAssessmentTask):
    """Detects the presence of phone numbers in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        cache: Cache,
        text_column: str = "content",
        new_column_name: str = "contains_phone_number",
    ):
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_phone_number)
        return result.rename(self.new_column_name)

    @staticmethod
    def _contains_phone_number(text):
        """
        Detects if the given text contains a phone number.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains a phone number, False otherwise.
        """
        phone_number_pattern = r"\b(?:\d{3}[-.]?|\(\d{3}\) )?\d{3}[-.]?\d{4}\b"
        return bool(re.search(phone_number_pattern, text))


# ------------------------------------------------------------------------------------------------ #
#                               DETECT OUTLIERS TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectOutliersTask(DataQualityAssessmentTask):
    def __init__(self, cache: Cache, column_name: str, new_column_name: str):
        """Detects outliers in vote sum, vote count, and review length columns.

        Args:
            cache (Cache): Cache for storing interim results.
            column_name (str): Column to examine
            new_column_name (str): Column containing indicator of presence of outliers.
        """
        super().__init__(cache=cache, new_column_name=new_column_name)
        self._column_name = column_name

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """Detects outliers in three columns

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = self._check_outliers(data)

        return pd.Series(data=result, name=self.new_column_name)

    def _check_outliers(self, data: pd.DataFrame):
        """
        Detects if the given text contains an email address.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains an email address, False otherwise.
        """
        # Compute the inter-quartile range
        q1 = data[self._column_name].quantile(0.25)
        q3 = data[self._column_name].quantile(0.75)
        iqr = q3 - q1

        # Compute lower and upper thresholds
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)

        # Flag observations
        result = np.where(
            (data[self._column_name] < lower) | (data[self._column_name] > upper),
            True,
            False,
        )

        return result
