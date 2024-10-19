#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/dqa.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Saturday October 19th 2024 06:20:32 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
import os
import re
import warnings
from collections import Counter
from typing import AnyStr, List, Optional, Union

import emoji
import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from lingua import Language, LanguageDetectorBuilder
from pandarallel import pandarallel
from profanity_check import predict
from sklearn.exceptions import InconsistentVersionWarning
from sklearn.utils._testing import ignore_warnings
from tqdm import tqdm

from discover.infra.service.cache.cachenow import cachenow
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.data.dataframe import split_dataframe
from discover.orchestration.base.task import Task

warnings.filterwarnings("ignore")
# warnings.simplefilter("ignore")
# warnings.filterwarnings("ignore", category=InconsistentVersionWarning)
# warnings.filterwarnings("ignore", message="load_model does not return WordVectorModel")
# warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
os.environ["PYTHONWARNINGS"] = "ignore"

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=12, verbose=False)
# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()


# ------------------------------------------------------------------------------------------------ #
#                                       ENTROPY                                                    #
# ------------------------------------------------------------------------------------------------ #
class EntropyTask(Task):
    """ """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "entropy",
        threshold: float = 4.75,
    ) -> None:
        """"""
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._threshold = threshold

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """"""
        result = data[self._text_column].parallel_apply(self._calculate_entropy)

        data[self._dqa_column] = result.gt(self._threshold)
        return data

    @staticmethod
    def _calculate_entropy(text: str) -> float:
        # Count frequency of each character in the text
        freq = Counter(text)

        # Calculate the probabilities of each character
        total_chars = len(text)
        probabilities = [count / total_chars for count in freq.values()]

        # Calculate Shannon entropy
        entropy = -sum(p * np.log2(p) for p in probabilities if p > 0)

        return entropy


# ------------------------------------------------------------------------------------------------ #
#                                       DUPLICATE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateTask(Task):
    """
    A task that detects duplicate rows in a pandas DataFrame based on specified column(s) and adds a column flagging duplicates.

    Args:
        dqa_column (str): The name of the column to store the duplicate flag results. The new column will contain boolean values indicating whether the row is a duplicate.
        column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) used to detect duplicates. If None, all columns are considered. Defaults to None.

    Attributes:
        _column_names (Optional[Union[List[AnyStr], AnyStr]]): The column(s) to check for duplicates.
        _dqa_column (str): The name of the column where the duplicate flag is stored.
    """

    def __init__(
        self,
        dqa_column: str,
        column_names: Optional[Union[List[AnyStr], AnyStr]] = None,
    ) -> None:
        """
        Initializes the DetectDuplicateTask with the specified column(s) to check for duplicates and the name of the column to store the results.

        Args:
            dqa_column (str): The column name where the duplicate flag will be stored.
            column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) to use for detecting duplicates. If None, the entire row will be checked for duplicates. Defaults to None.
        """
        super().__init__()
        self._column_names = column_names
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects duplicate rows based on the specified columns and creates a new column to flag duplicates.

        Args:
            data (pd.DataFrame): The input DataFrame to check for duplicates.

        Returns:
            pd.DataFrame: A DataFrame with a new column indicating whether each row is a duplicate, based on the specified columns.
        """
        data[self._dqa_column] = data.duplicated(
            subset=self._column_names, keep="first"
        )
        return data


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT NULL VALUES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(Task):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(
        self,
        dqa_column: str = "has_null",
    ):
        super().__init__()
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """
        data[self._dqa_column] = data.isnull().any(axis=1)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT INVALID RATINGS                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(Task):
    """"""

    def __init__(
        self, dqa_column: str = "invalid_rating", rating_column: str = "rating"
    ):
        super().__init__()
        self._dqa_column = dqa_column
        self._rating_column = rating_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """
        data[self._dqa_column] = ~data[self._rating_column].between(0, 5)
        return data


# ------------------------------------------------------------------------------------------------ #
#                            DETECT NON-ENGLISH TASK                                               #
# ------------------------------------------------------------------------------------------------ #
# Standalone function for processing a chunk
@ignore_warnings(category=InconsistentVersionWarning)
def process_chunk(chunk, text_column, new_column, model_filepath):
    # Use catch_warnings and filterwarnings inside the context manager
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        warnings.filterwarnings("ignore")
        warnings.filterwarnings(
            "ignore", category=InconsistentVersionWarning, module="sklearn"
        )
        warnings.simplefilter("ignore", InconsistentVersionWarning)
        warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

        # Your model loading or FastText code that triggers the warnings
        import fasttext

        fasttext.FastText.eprint = lambda x: None

        model = fasttext.load_model(model_filepath)
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


# Function to re-evaluate potentially non-English texts
def re_evaluate(text):
    try:
        return detector.detect_language_of(text) == Language.ENGLISH
    except Exception as e:
        print(f"Error in re-evaluation: {e}")
        return False


class DetectNonEnglishTask(Task):
    """Detects which reviews contain non-English text.

    Args:
        text_column (str): Name of the column containing text to be analyzed.
        new_column_name (str): Name of the new column to be created indicating whether the text is in English or not.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_non_english",
        n_jobs: int = 12,
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        # Split data into chunks
        chunks = split_dataframe(data=data, n=self._n_jobs)

        # Process chunks in parallel using joblib
        results = Parallel(n_jobs=self._n_jobs)(
            delayed(process_chunk)(
                chunk, self._text_column, self._dqa_column, self._model_filepath
            )
            for chunk in tqdm(chunks)
        )

        data[self._dqa_column] = pd.concat(results, axis=0)

        # Apply re-evaluation only to rows where 'is_non_english' is True
        data.loc[data[self._dqa_column], self._dqa_column] = data.loc[
            data[self._dqa_column], self._text_column
        ].parallel_apply(lambda text: not re_evaluate(text))

        return data


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMOJI TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_has_emoji",
    ):
        """
        Initializes the DetectEmojiTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        data[self._dqa_column] = data[self._text_column].parallel_apply(
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
#                            DETECT SPECIAL CHARACTERS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectSpecialCharacterTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_excessive_special_chars",
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
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._threshold = threshold

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        data[self._dqa_column] = data[self._text_column].parallel_apply(
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
#                               DETECT INVALID DATES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidDatesTask(Task):
    def __init__(
        self, date_column: str, dqa_column: str, start_date: int, end_date: int
    ):
        """
        Detects dates that are outside the range of 2008-2023

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__()
        self._date_column = date_column
        self._dqa_column = dqa_column
        self._start_date = start_date
        self._end_date = end_date

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid dates and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid dates.

        """

        data[self._dqa_column] = data[self._date_column].parallel_apply(
            self._date_invalid
        )
        return data

    def _date_invalid(self, date: np.datetime64) -> bool:
        """Indicates whether the rating is on the five point scale."""
        date = np.datetime64(date)

        return date < np.datetime64(str(self._start_date)) or date > np.datetime64(
            str(self._end_date)
        )


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
class DetectProfanityTask(Task):
    """Detects profanity in the designated column of a dataframe.

    Note: This class leverages the multiprocessing module. To avoid unintended behavior and
    recursively spawning processes, this should be executed with the __main__ shield. This
    is not strictly required in a WSL2 virtual machine, but should be refactored behind
    a __main__  shield for cross-platform portability.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_has_profanity",
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
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._n_jobs = n_jobs

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect profanity in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """
        chunks = split_dataframe(data=data, n=100)
        results = [
            chunk.parallel_apply(
                lambda row: detect_profanity(row[self._text_column]), axis=1
            )
            for chunk in tqdm(chunks)
        ]
        # Concatenate the processed chunks
        data[self._dqa_column] = pd.concat(results, axis=0)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMAIL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "contains_email",
    ):
        """Initializes the DetectSpecialPatternsTask with the column name containing text data.


        Args:
            text_column (str): Name of the column containing text to be analyzed.
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects special patterns (emails, URLs, phone numbers) in the specified text column
        and adds new columns indicating the presence of each pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self._dqa_column] = data[self._text_column].parallel_apply(
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
#                                  DETECT URL TASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class DetectURLTask(Task):
    """Detects the presence of URLs in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_contains_url",
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self._dqa_column] = data[self._text_column].parallel_apply(
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
#                               DETECT PHONE NUMBER TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectPhoneNumberTask(Task):
    """Detects the presence of phone numbers in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_contains_phone_number",
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        data[self._dqa_column] = data[self._text_column].parallel_apply(
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
