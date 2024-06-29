#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/data_prep/dqa.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Friday June 28th 2024 07:52:27 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Module"""
import os
import re
from dotenv import load_dotenv
import warnings
from dataclasses import dataclass
import shelve
from typing import AnyStr, List, Optional, Union

from joblib import Parallel, delayed
from tqdm import tqdm
import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler, ProgressBar
import emoji
import fasttext
import numpy as np
import pandas as pd
from pandarallel import pandarallel
from profanity_check import predict

from appinsight.data_prep import log_exceptions, task_profiler
from appinsight.data_prep.base import Preprocessor
from appinsight.data_prep.io import ReadTask, WriteTask
from appinsight.utils.base import Reader, Writer
from appinsight.utils.io import FileReader, FileWriter
from appinsight.utils.repo import DatasetRepo
from appinsight.workflow.config import StageConfig
from appinsight.workflow.pipeline import Pipeline
from appinsight.workflow.task import Task


# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQAConfig(StageConfig):
    """Class encapsulating the configuration for data quality assessment stage."""

    name: str = "DataQualityAssessment"
    source_directory: str = "01_norm/reviews"
    source_filename: str = None
    target_directory: str = "02_dqa/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    n_jobs: int = 12
    n_partitions: int = 50
    force: bool = False


# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessment(Preprocessor):
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
        dsm_cls (type[DatasetRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
    """

    def __init__(
        self,
        config: DQAConfig,
        source_reader_cls: type[Reader] = FileReader,
        target_writer_cls: type[Writer] = FileWriter,
        target_reader_cls: type[Reader] = FileReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        dsm_cls: type[DatasetRepo] = DatasetRepo,
        fastmode: bool = False,
    ) -> None:
        """Initializes the DataQualityPipeline with data."""
        super().__init__(
            config=config,
            source_reader_cls=source_reader_cls,
            target_writer_cls=target_writer_cls,
            target_reader_cls=target_reader_cls,
            pipeline_cls=pipeline_cls,
            dsm_cls=dsm_cls,
        )
        self._fastmode = fastmode
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

        # Instantiate duplicate checkers
        dup1 = DetectDuplicateRowTask(new_column_name="is_duplicate")
        dup2 = DetectDuplicateRowTask(
            column_names="id", new_column_name="is_duplicate_review_id"
        )
        # Instantiate detection of null values
        null = DetectNullValuesTask(new_column_name="has_null_values")

        # Instantiate detection of outliers
        out_vote_sum = DetectOutliersTask(
            column_name="vote_sum", new_column_name="vote_sum_outlier"
        )
        out_vote_count = DetectOutliersTask(
            column_name="vote_count", new_column_name="vote_count_outlier"
        )
        # Instantiate detection of non-english text in reviews and app names.
        lang_review = DetectNonEnglishTask(
            text_column="content",
            new_column_name="has_non_english_review",
            n_jobs=self.config.n_jobs,
        )
        lang_app_name = DetectNonEnglishTask(
            text_column="app_name",
            new_column_name="has_non_english_app_name",
            n_jobs=self.config.n_jobs,
        )
        # Instantiate detection of emojis
        emoji = DetectEmojiTask(new_column_name="has_emojis")
        # Instantiate detection of excessive special characters
        chars = DetectSpecialCharacterTask(
            threshold=0.3, new_column_name="has_excessive_special_chars"
        )
        # Instantiate date validator
        dates = DetectInvalidDatesTask(new_column_name="has_invalid_date")
        # Instantiate a rating validator
        ratings = DetectInvalidRatingsTask(new_column_name="has_invalid_rating")
        # Instantiate a profanity check
        profanity = DetectProfanityTask(
            n_jobs=self.config.n_jobs,
            n_partitions=self.config.n_partitions,
            new_column_name="has_profanity",
        )
        # Instantiate email, URL, and phone number detection in review text.
        emails = DetectEmailTask(new_column_name="contains_email")
        urls = DetectURLTask(new_column_name="contains_url")
        phones = DetectPhoneNumberTask(new_column_name="contains_phone_number")

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(dup1)
        pipe.add_task(dup2)
        pipe.add_task(null)
        pipe.add_task(out_vote_sum)
        pipe.add_task(out_vote_count)
        pipe.add_task(lang_review)
        pipe.add_task(lang_app_name)
        pipe.add_task(emoji)
        pipe.add_task(chars)
        pipe.add_task(dates)
        pipe.add_task(ratings)
        if not self._fastmode:
            pipe.add_task(profanity)
        pipe.add_task(emails)
        pipe.add_task(urls)
        pipe.add_task(phones)
        pipe.add_task(save)
        return pipe


# ------------------------------------------------------------------------------------------------ #
#                               DATA QUALITY ASSESSMENT TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentTask(Task):
    """Base class for data quality tasks.

    Args:
        new_column_name (str): The column name used as an indicator of the data quality element.
    """

    PREFIX = "dqa_"

    def __init__(self, new_column_name: str):
        super().__init__()
        self._new_column_name = f"{self.PREFIX}{new_column_name}"
        # Cache related variables and operations
        self._cache_key = self.__class__.__name__.lower()
        self._cache_filepath = f"cache/{os.getenv('ENV')}/dqa"
        os.makedirs(os.path.dirname(self._cache_filepath), exist_ok=True)

    @property
    def new_column_name(self) -> str:
        """Returns the new column a data quality assessment subtask adds to the dataset."""
        return self._new_column_name

    def execute(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the underlying subtask's execute_task method if result not in cache.

        Args:
            data (pd.DataFrame): Data to be assessed.
        """
        self.start_task()
        # Try to obtain the result from cache.
        try:
            result = self._get_cache(key=self._cache_key)
            data = pd.concat([data, result.rename(self.new_column_name)], axis=1)

        # Otherwise, obtain result by executing the task, then save result to cache.
        except KeyError:
            data = self.execute_task(data=data)
            self._cache(key=self._cache_key, value=data[self.new_column_name])

        except Exception as e:
            self._logger.exception(
                f"Exception occurred while executing {self.__class__.__name__}\n{e}"
            )
            raise
        self.stop_task()
        return data

    def _cache(self, key: str, value: pd.Series) -> None:
        """Caches a result"""
        with shelve.open(self._cache_filepath) as cache:
            cache[key] = value

    def _get_cache(self, key: str) -> pd.Series:
        """Gets cached result"""
        with shelve.open(self._cache_filepath) as cache:
            return cache[key]


# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateRowTask(DataQualityAssessmentTask):
    """A task to mark duplicate rows in a DataFrame.

    Attributes:
        column_names (list): A list of column names to consider when identifying duplicates.
        new_column_name (str): The name of the column to add, indicating whether each row is a duplicate.
    """

    def __init__(
        self,
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
        super().__init__(new_column_name=new_column_name)
        self._column_names = column_names

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark duplicate rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is a duplicate.
        """
        # Check for duplicates.
        data[self.new_column_name] = data.duplicated(
            subset=self._column_names, keep="first"
        )

        return data


# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(DataQualityAssessmentTask):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(
        self,
        new_column_name: str = "has_null",
    ):
        super().__init__(new_column_name=new_column_name)

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """

        data[self.new_column_name] = data.isnull().values.any(axis=1)

        return data


# ------------------------------------------------------------------------------------------------ #
# Standalone function for processing a chunk
def process_chunk(chunk, text_column, new_column, model_path):
    model = fasttext.load_model(model_path)  # Load the model in each worker
    chunk[new_column] = chunk[text_column].apply(
        lambda text: is_non_english(text, model)
    )
    return chunk


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
        text_column: str = "content",
        new_column_name: str = "non_english",
        n_jobs: int = 12,
    ):
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        # Split data into chunks
        chunks = np.array_split(data, self._n_jobs or 1)
        self._logger.debug(f"Data split into {len(chunks)} chunks")

        # Process chunks in parallel using joblib
        results = Parallel(n_jobs=self._n_jobs)(
            delayed(process_chunk)(
                chunk, self._text_column, self.new_column_name, self._model_filepath
            )
            for chunk in tqdm(chunks)
        )

        # Concatenate the processed chunks
        return pd.concat(results, ignore_index=True, axis=0)


# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(DataQualityAssessmentTask):
    def __init__(
        self, text_column: str = "content", new_column_name: str = "has_emoji"
    ):
        """
        Initializes the DetectEmojiTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
        """
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        data[self.new_column_name] = data[self._text_column].parallel_apply(
            self._contains_emoji
        )

        return data

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
class DetectSpecialCharacterTask(DataQualityAssessmentTask):
    def __init__(
        self,
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
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column
        self._threshold = threshold

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        data[self.new_column_name] = data[self._text_column].parallel_apply(
            self._contains_excessive_special_chars
        )

        return data

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
class DetectInvalidDatesTask(DataQualityAssessmentTask):
    def __init__(self, new_column_name: str = "date_invalid"):
        """
        Detects dates that are outside the range of 2008-2023

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__(new_column_name=new_column_name)

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid dates and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """

        data[self.new_column_name] = data["date"].parallel_apply(self._date_invalid)

        return data

    def _date_invalid(self, date: np.datetime64) -> bool:
        """Indicates whether the rating is on the five point scale."""
        date = np.datetime64(date)

        return date < np.datetime64("2007") or date > np.datetime64("2024")


# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(DataQualityAssessmentTask):
    def __init__(self, new_column_name: str = "rating_invalid"):
        """
        Detects ratings that are not on the five-point scale.

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__(new_column_name=new_column_name)

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid ratings and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """

        data[self.new_column_name] = data["rating"].parallel_apply(self._rating_invalid)

        return data

    def _rating_invalid(self, rating: int) -> bool:
        """Indicates whether the rating is on the five point scale."""
        return rating < 0 or rating > 5


# ------------------------------------------------------------------------------------------------ #
class DetectProfanityTask(DataQualityAssessmentTask):
    def __init__(
        self,
        text_column: str = "content",
        new_column_name: str = "has_profanity",
        n_jobs: int = 12,
        n_partitions: int = 50,
    ):
        """
        Initializes the DetectProfanityTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating
                whether the text contains profanity or not.
            n_jobs (int): Number of cpus corresponding to 'jobs' in a concurrent context.
        """
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column
        self._n_jobs = n_jobs
        self._n_partitions = n_partitions
        # Start a Dask client for a local environment using processes
        client = Client(processes=True, n_workers=n_jobs, threads_per_worker=1)
        print("Dask dashboard available at:", client.dashboard_link)

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """
        Executes the task to detect profanity in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """

        # Convert pandas to Dask
        df = dd.from_pandas(data, npartitions=self._n_partitions)

        # Use dask apply
        result = df[self._text_column].apply(
            self._contains_profanity, meta=(self._text_column, "bool")
        )

        # Persist result to avoid recomputation
        with Profiler() as prof, ResourceProfiler() as rprof, CacheProfiler() as cprof, ProgressBar():
            result = result.compute()

        data[self.new_column_name] = result

        # Visualize profiling results
        prof.visualize()
        rprof.visualize()
        cprof.visualize()

        return data

    @staticmethod
    def _contains_profanity(text):
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
            # Use a simple print statement for error logging in parallel function
            print(f"Error in profanity detection: {e}")
            return False


# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(DataQualityAssessmentTask):
    def __init__(
        self, text_column: str = "content", new_column_name: str = "contains_email"
    ):
        """Initializes the DetectSpecialPatternsTask with the column name containing text data.


        Args:
            text_column (str): Name of the column containing text to be analyzed.
        """
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """Detects special patterns (emails, URLs, phone numbers) in the specified text column
        and adds new columns indicating the presence of each pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self.new_column_name] = data[self._text_column].parallel_apply(
            self._contains_email
        )
        return data

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
class DetectURLTask(DataQualityAssessmentTask):
    """Detects the presence of URLs in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self, text_column: str = "content", new_column_name: str = "contains_url"
    ):
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self.new_column_name] = data[self._text_column].parallel_apply(
            self._contains_url
        )
        return data

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
class DetectPhoneNumberTask(DataQualityAssessmentTask):
    """Detects the presence of phone numbers in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        text_column: str = "content",
        new_column_name: str = "contains_phone_number",
    ):
        super().__init__(new_column_name=new_column_name)
        self._text_column = text_column

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self.new_column_name] = data[self._text_column].parallel_apply(
            self._contains_phone_number
        )
        return data

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


class DetectOutliersTask(DataQualityAssessmentTask):
    def __init__(self, column_name: str, new_column_name: str):
        """Detects outliers in vote sum, vote count, and review length columns.


        Args:
            columns (list): List of columns to examine.
        """
        super().__init__(new_column_name=new_column_name)
        self._column_name = column_name

    @task_profiler()
    @log_exceptions()
    def execute_task(self, data: pd.DataFrame):
        """Detects outliers in three columns

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data = self._check_outliers(data)

        return data

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
        data[self.new_column_name] = np.where(
            (data[self._column_name] < lower) | (data[self._column_name] > upper),
            True,
            False,
        )

        return data
