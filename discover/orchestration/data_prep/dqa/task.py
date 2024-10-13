#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/dqa/task.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 08:38:32 pm                                           #
# Modified   : Sunday October 13th 2024 01:57:30 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Module"""
from __future__ import annotations

import re
import warnings

import emoji
import fasttext
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from pandarallel import pandarallel
from profanity_check import predict
from tqdm import tqdm

from discover.dynamics.base.task import Task, TaskConfig
from discover.infra.utils.data.dataframe import split_dataframe

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
fasttext.FastText.eprint = lambda x: None


# ------------------------------------------------------------------------------------------------ #
#                               DATA QUALITY ASSESSMENT TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentTask(Task):
    """Base class for data quality tasks.

    Args:
        new_column_name (str): The column name used as an indicator of the data quality element.
    """

    PREFIX = "dqa_"

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    @property
    def new_column_name(self) -> str:
        """Returns the new column a data quality assessment subtask adds to the dataset."""
        return f"{self.PREFIX}{self._config.new_column_name}"


# ------------------------------------------------------------------------------------------------ #
#                               DETECT DUPLICATE ROW TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateRowTask(DataQualityAssessmentTask):
    """A task to mark duplicate rows in a DataFrame.

    Attributes:
        column_names (list): A list of column names to consider when identifying duplicates.
        new_column_name (str): The name of the column to add, indicating whether each row is a duplicate.
    """

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._column_names = config.column_names

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
        result = result.rename(self.new_column_name)
        return (
            result
            if isinstance(result, (pd.DataFrame, pd.core.frame.DataFrame))
            else result.to_frame(name=self.new_column_name)
        )


# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateReviewIdTask(DetectDuplicateRowTask):
    """Detects duplicate review identifiers in the dataset."""


# ------------------------------------------------------------------------------------------------ #
#                                 DATA NULL VALUES TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(DataQualityAssessmentTask):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """

        result = data.isnull().values.any(axis=1)
        result = pd.Series(data=result, name=self.new_column_name)
        return result.to_frame(name=self.new_column_name)


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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._n_jobs = config.n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = config.fasttext_model_filepath

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
                chunk,
                self._config.text_column,
                self.new_column_name,
                self._model_filepath,
            )
            for chunk in tqdm(chunks)
        )

        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMOJI TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(DataQualityAssessmentTask):

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._config.text_column].parallel_apply(self._contains_emoji)

        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._threshold = config.threshold

    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._config.text_column].parallel_apply(
            self._contains_excessive_special_chars
        )

        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._start_year = config.start_year
        self._current_year = config.current_year

    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid dates and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid dates.

        """

        result = data["date"].parallel_apply(self._date_invalid)

        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

    def _date_invalid(self, date: np.datetime64) -> bool:
        """Indicates whether the rating is on the five point scale."""
        date = np.datetime64(date)

        return date < np.datetime64(self._start_year) or date > np.datetime64(
            self._current_year
        )


# ------------------------------------------------------------------------------------------------ #
#                               DETECT INVALID RATINGS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(DataQualityAssessmentTask):

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

        self._rating_low = config.rating_low
        self._rating_high = config.rating_high

    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid ratings and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid ratings.

        """

        result = data["rating"].parallel_apply(self._rating_invalid)

        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

    def _rating_invalid(self, rating: int) -> bool:
        """Indicates whether the rating is on the five point scale."""
        return rating < self._rating_low or rating > self._rating_high


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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._n_jobs = config.n_jobs

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
                lambda row: detect_profanity(row[self._config.text_column]), axis=1
            )
            for chunk in tqdm(chunks)
        ]
        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMAIL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(DataQualityAssessmentTask):

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    def run(self, data: pd.DataFrame):
        """Detects special patterns (emails, URLs, phone numbers) in the specified text column
        and adds new columns indicating the presence of each pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._config.text_column].parallel_apply(self._contains_email)
        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._config.text_column].parallel_apply(self._contains_url)
        result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)

    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._config.text_column].parallel_apply(
            self._contains_phone_number
        )
        # result = result.rename(self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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

    def __init__(self, config: TaskConfig):
        super().__init__(config=config)
        self._column_name = config.column_name

    def run(self, data: pd.DataFrame):
        """Detects outliers in three columns

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = self._check_outliers(data)

        result = pd.Series(data=result, name=self.new_column_name)
        return result.to_frame(name=self.new_column_name)

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


# ------------------------------------------------------------------------------------------------ #
class DetectVoteCountOutliersTask(DetectOutliersTask):
    """Detects VoteCount Outliers"""


# ------------------------------------------------------------------------------------------------ #
class DetectVoteSumOutliersTask(DetectOutliersTask):
    """Detects VoteSum Outliers"""
