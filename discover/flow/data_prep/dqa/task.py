#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/dqa/task.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Monday October 21st 2024 12:06:20 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
import os
import re
import warnings
from abc import abstractmethod
from collections import Counter
from typing import Any, Optional

import emoji
import fasttext
import numpy as np
import pandas as pd
from lingua import Language, LanguageDetectorBuilder
from pandarallel import pandarallel
from profanity_check import predict
from symspellpy import SymSpell, Verbosity

from discover.flow.base.task import Task
from discover.infra.service.cache.cachenow import cachenow
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=12, verbose=False)

# ------------------------------------------------------------------------------------------------ #
#                     LANGUAGE MODELS FOR LANGUAGE DETECTION                                       #
# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None  # Suppress FastText warnings
fasttext_model = fasttext.load_model("models/language_detection/lid.176.bin")
# ------------------------------------------------------------------------------------------------ #
sym_spell = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)
sym_spell.load_dictionary(
    "models/spelling/frequency_dictionary_en_82_765.txt", term_index=0, count_index=1
)


# ------------------------------------------------------------------------------------------------ #
#                                      DQA TASK                                                    #
# ------------------------------------------------------------------------------------------------ #
class DQATask(Task):
    """
    A base class for data quality assurance (DQA) tasks.

    This class provides the foundation for tasks that assess data quality on a specified column and
    flag anomalies in a corresponding DQA column. Subclasses must implement the core logic of the
    task in the `run` method.

    Attributes:
    -----------
    dqa_column : str
        The name of the column where the results of the DQA check (anomalies) will be stored.
    column : str
        The name of the column that the DQA task will inspect. Optional. If not provided, all
            columns are evaluated.


    Methods:
    --------
    run(*args, data: Any, **kwargs) -> Any
        Abstract method that must be implemented by subclasses to define the core logic of the task.
    summarize(data: pd.DataFrame) -> None
        Summarizes the number of anomalies detected by the task and prints a report.
    """

    def __init__(self, dqa_column: str = None, column: Optional[str] = None):
        """
        Initializes the DQATask with the column to inspect and the column to store the results.

        Args:
        -----
        column : str
            The name of the column to inspect for data quality issues.
        dqa_column : str
            The name of the column where the anomalies will be flagged.
        """
        super().__init__()
        self._dqa_column = dqa_column
        self._column = column

    @property
    def column(self) -> str:
        """Returns the name of the column to inspect."""
        return self._column

    @property
    def dqa_column(self) -> str:
        """Returns the name of the column where anomalies are flagged."""
        return self._dqa_column

    @abstractmethod
    def run(self, *args, data: Any, **kwargs) -> Any:
        """
        The core logic of the task. Must be implemented by any subclass.

        Parameters:
        -----------
        *args : tuple
            Positional arguments that the task may require.
        data : Any
            The input data for the task. The specific type of data will depend
            on the implementation of the subclass.
        **kwargs : dict
            Additional keyword arguments that the task may require.

        Returns:
        --------
        Any
            The output of the task, as defined by the subclass implementation.
        """
        pass

    def summarize(self, data: pd.DataFrame) -> None:
        """
        Summarizes the anomalies detected by the DQA task and prints the results.

        This method calculates the total number of anomalies detected in the `dqa_column`
        and provides a percentage of how many records contain anomalies.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame containing the data and the corresponding DQA results.

        Returns:
        --------
        None
        """
        anomalies = data.sum()
        n = data.shape[0]
        p = round(anomalies / n * 100, 2)
        print(
            f"\t{self.__class__.__name__} detected {anomalies} anomalies in the {self.dqa_column} column, {p}% of {n} records."
        )


# ------------------------------------------------------------------------------------------------ #
#                                       ENTROPY                                                    #
# ------------------------------------------------------------------------------------------------ #
class EntropyTask(DQATask):
    """
    A data quality assurance (DQA) task that checks for high entropy in a specified text column.

    This task calculates the Shannon entropy of text data in a specified column and flags entries
    where the entropy exceeds a predefined threshold. High entropy often indicates more complex
    or less predictable text, which might be used as a signal for certain quality checks.

    Attributes:
    -----------
    _column : str
        The name of the column containing the text data to inspect.
    _dqa_column : str
        The name of the column where the DQA results (entropy flags) will be stored.
    _threshold : float
        The threshold above which text entries are flagged for high entropy.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the entropy check and flags entries where the entropy is greater than the threshold.
    _calculate_entropy(text: str) -> float
        A static method that calculates the Shannon entropy of a given text string.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "entropy",
        threshold: float = 4.75,
    ) -> None:
        """
        Initializes the EntropyTask with a target column, a DQA column for flags, and a threshold for entropy.

        Args:
        -----
        column : str, optional
            The name of the column containing the text data to inspect. Default is "content".
        dqa_column : str, optional
            The name of the column where the DQA results (entropy flags) will be stored. Default is "entropy".
        threshold : float, optional
            The entropy threshold above which text entries will be flagged. Default is 4.75.
        """
        super().__init__(column=column, dqa_column=dqa_column)
        self._threshold = threshold

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the entropy check on the specified column and flags entries with high entropy.

        This method calculates the Shannon entropy for each text entry in the specified column
        and flags those that exceed the defined threshold by setting the value in the DQA column.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the data to be analyzed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column that flags high entropy entries.
        """
        result = data[self._column].parallel_apply(self._calculate_entropy)

        result = result.gt(self._threshold)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    @staticmethod
    def _calculate_entropy(text: str) -> float:
        """
        Calculates the Shannon entropy of a given text string.

        The entropy is a measure of unpredictability or information content in the text.
        Higher entropy indicates more randomness or complexity in the text.

        Args:
        -----
        text : str
            The input text string to calculate entropy for.

        Returns:
        --------
        float
            The Shannon entropy of the input text.
        """
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
class DetectDuplicateTask(DQATask):
    """
    A task that detects duplicate rows in a pandas DataFrame based on specified column(s) and adds a column flagging duplicates.

    Args:
        dqa_column (str): The name of the column to store the duplicate flag results. The new column will contain boolean values indicating whether the row is a duplicate.
        column (Optional[str]): The column used to detect duplicates. If None, all columns are considered. Defaults to None.

    Attributes:
        column (Optional[str]):  The column(s) to check for duplicates.
        dqa_column (str): The name of the column where the duplicate flag is stored.
    """

    def __init__(
        self,
        dqa_column: str,
        column: Optional[str] = None,
    ) -> None:
        """
        Initializes the DetectDuplicateTask with the specified column(s) to check for duplicates and the name of the column to store the results.

        Args:
            dqa_column (str): The column name where the duplicate flag will be stored.
            column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) to use for detecting duplicates. If None, the entire row will be checked for duplicates. Defaults to None.
        """
        super().__init__(column=column, dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects duplicate rows based on the specified columns and creates a new column to flag duplicates.

        Args:
            data (pd.DataFrame): The input DataFrame to check for duplicates.

        Returns:
            pd.DataFrame: A DataFrame with a new column indicating whether each row is a duplicate, based on the specified columns.
        """
        result = data.duplicated(subset=self._column, keep="first")

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT NULL VALUES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(DQATask):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(
        self,
        dqa_column: str = "has_null",
    ):
        super().__init__(dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """
        result = data.isnull().any(axis=1)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT INVALID RATINGS                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(DQATask):
    """
    A data quality assurance (DQA) task that detects invalid ratings in a specified column.

    This task checks whether the ratings in the specified column fall outside the valid range (0 to 5).
    Entries with invalid ratings are flagged in a corresponding DQA column.

    Attributes:
    -----------
    _column : str
        The name of the column containing the ratings to be inspected.
    _dqa_column : str
        The name of the column where the DQA results (invalid rating flags) will be stored.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the invalid rating detection task and flags entries where ratings fall outside the valid range.
    """

    def __init__(self, dqa_column: str = "invalid_rating", column: str = "rating"):
        """
        Initializes the DetectInvalidRatingsTask with the target column for ratings and a DQA column for flags.

        Args:
        -----
        dqa_column : str, optional
            The name of the column where the DQA results (invalid rating flags) will be stored. Default is "invalid_rating".
        column : str, optional
            The name of the column containing the ratings to be inspected. Default is "rating".
        """
        super().__init__(column=column, dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Marks rows in the DataFrame where the rating is invalid (not between 0 and 5).

        This method adds a new column to the DataFrame that flags invalid ratings.
        Ratings that fall outside the valid range (0 to 5) are flagged as True.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the data to be analyzed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column indicating invalid ratings.
        """
        result = ~data[self._column].between(0, 5)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result


# ------------------------------------------------------------------------------------------------ #
#                            DETECT NON-ENGLISH TASK                                               #
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
def lm_fasttext(text):
    """
    Determines if a given text is non-English using the FastText model.

    This function predicts the language of the input text and classifies it as non-English if
    the predicted label is not English (`__label__en`) and the probability of being English
    is below a threshold of 90%.

    Args:
    -----
    text : str
        The input text to be analyzed.

    Returns:
    --------
    bool
        True if the text is non-English, False otherwise.
    """
    try:
        predictions = fasttext_model.predict(text)
        return predictions[0][0] != "__label__en"
    except Exception as e:
        print(f"Error in language detection: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #
def lm_lingua(text):
    """
    Re-evaluates potentially non-English text using a secondary language detection method.

    This function uses an additional language detection tool (e.g., `lingua`) to double-check
    whether the input text is English. The text is classified as non-English (True) if the detection
    does not return English.

    Args:
    -----
    text : str
        The input text to be re-evaluated.

    Returns:
    --------
    bool
        True if the text is non-English, False otherwise.
    """
    try:
        return detector.detect_language_of(text) != Language.ENGLISH
    except Exception as e:
        print(f"Error in re-evaluation: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #
class DetectNonEnglishTask(DQATask):
    """Detects non-English text.

    Args:
        column (str): Name of the column containing text to be analyzed.
        new_column_name (str): Name of the new column to be created indicating whether the text is in English or not.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_non_english",
        n_jobs: int = 12,
    ):
        super().__init__(column=column, dqa_column=dqa_column)
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        return self._run_both(data=data)

    def _run_lingua(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        result = data[self._column].parallel_apply(lm_lingua)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    def _run_fasttext(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        result = data[self._column].parallel_apply(lm_fasttext)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    def _run_both(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        # Make a copy of the data
        df = data.copy()

        # Conduct primary language detection
        df[self._dqa_column] = data[self._column].parallel_apply(lm_fasttext)

        # Apply re-evaluation only to rows where 'is_non_english' is True
        df.loc[df[self._dqa_column], self._dqa_column] = df.loc[
            df[self._dqa_column], self._column
        ].parallel_apply(lambda text: lm_lingua(text))

        result = df[self._dqa_column]

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMOJI TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(DQATask):
    """
    A data quality assurance (DQA) task that detects whether a given text contains emojis.

    This task inspects a specified column in a DataFrame and flags entries that contain emojis.
    The results are stored in a new DQA column that indicates the presence of emojis in each text.

    Attributes:
    -----------
    _column : str
        The name of the column containing the text to be inspected for emojis.
    _dqa_column : str
        The name of the column where the results (emoji flags) will be stored.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the task to detect emojis in the specified column and flags entries in the DQA column.
    _contains_emoji(text: str) -> bool
        A static method that checks if a given text contains any emojis.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_has_emoji",
    ):
        """
        Initializes the DetectEmojiTask with the specified text column and DQA column.

        Args:
        -----
        column : str, optional
            The name of the column containing the text to be analyzed. Default is "content".
        dqa_column : str, optional
            The name of the column where the results (emoji flags) will be stored. Default is "b_has_emoji".
        """
        super().__init__(column=column, dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the emoji detection task and flags rows where the text contains emojis.

        This method adds a new column to the DataFrame that flags rows where emojis are detected
        in the specified text column.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the text data to be processed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column indicating whether each text contains emojis.
        """
        result = data[self._column].parallel_apply(self._contains_emoji)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    @staticmethod
    def _contains_emoji(text: str) -> bool:
        """
        Detects if the given text contains any emojis.

        This static method checks whether any characters in the input text are emojis.

        Args:
        -----
        text : str
            The input text to be analyzed.

        Returns:
        --------
        bool
            True if the text contains emojis, False otherwise.
        """
        return any(char in emoji.EMOJI_DATA for char in text)


# ------------------------------------------------------------------------------------------------ #
#                            DETECT SPECIAL CHARACTERS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectSpecialCharacterTask(DQATask):
    """
    A data quality assurance (DQA) task that flags rows with excessive special characters in the text.

    This task inspects a specified column in a DataFrame and flags entries that contain a high proportion
    of special characters (e.g., punctuation, symbols). Excessive special characters are defined as
    a proportion of the overall text length, with the threshold customizable.

    Attributes:
    -----------
    _column : str
        The name of the column containing the text to be inspected.
    _dqa_column : str
        The name of the column where the results (special character flags) will be stored.
    _threshold : float
        The proportion of special characters relative to the total text length, above which the text
        is considered to contain excessive special characters.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the task to detect texts with excessive special characters and flags them in the DQA column.
    _contains_excessive_special_chars(text: str) -> bool
        A helper method that calculates whether the proportion of special characters in the text
        exceeds the defined threshold.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_excessive_special_chars",
        threshold: float = 0.2,
    ):
        """
        Initializes the DetectSpecialCharacterTask with the specified column, DQA column, and threshold.

        Args:
        -----
        column : str, optional
            The name of the column containing the text to be analyzed. Default is "content".
        dqa_column : str, optional
            The name of the column where the results (special character flags) will be stored.
            Default is "b_excessive_special_chars".
        threshold : float, optional
            The proportion of the text that can be made up of special characters before it is flagged as
            excessive. Default is 0.2 (20% of the text).
        """
        super().__init__(column=column, dqa_column=dqa_column)
        self._threshold = threshold

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the task to detect texts with excessive special characters.

        This method applies the special character detection logic to each row in the DataFrame
        and flags rows where the proportion of special characters exceeds the specified threshold.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the text data to be analyzed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column indicating whether the text contains
            an excessive proportion of special characters.
        """
        result = data[self._column].parallel_apply(
            self._contains_excessive_special_chars
        )

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    def _contains_excessive_special_chars(self, text: str) -> bool:
        """
        Detects if the given text contains an excessive number of special characters.

        This method calculates the proportion of special characters in the text relative to
        its length. If the proportion exceeds the threshold, the text is flagged.

        Args:
        -----
        text : str
            The input text to be analyzed.

        Returns:
        --------
        bool
            True if the text contains excessive special characters, False otherwise.
        """
        special_chars = re.findall(r"[^\w\s]", text)
        return (
            len(special_chars) / len(text) > self._threshold if len(text) > 0 else False
        )


# ------------------------------------------------------------------------------------------------ #
#                               DETECT INVALID DATES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidDatesTask(DQATask):
    """
    A data quality assurance (DQA) task that detects invalid dates in a specified column.

    This task checks whether dates in the specified column fall outside the valid range (e.g., 2008-2023).
    Entries with invalid dates are flagged in a corresponding DQA column.

    Attributes:
    -----------
    _column : str
        The name of the column containing the dates to be inspected.
    _dqa_column : str
        The name of the column where the DQA results (invalid date flags) will be stored.
    _start_date : int
        The starting year of the valid date range (e.g., 2008).
    _end_date : int
        The ending year of the valid date range (e.g., 2023).

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the task to detect invalid dates and flags entries where the date is outside the valid range.
    _date_invalid(date: np.datetime64) -> bool
        A helper method that checks if a given date falls outside the valid date range.
    """

    def __init__(self, column: str, dqa_column: str, start_date: int, end_date: int):
        """
        Initializes the DetectInvalidDatesTask with the specified column, DQA column, and date range.

        Args:
        -----
        column : str
            The name of the column containing the dates to be analyzed.
        dqa_column : str
            The name of the column where the results (invalid date flags) will be stored.
        start_date : int
            The starting year of the valid date range (e.g., 2008).
        end_date : int
            The ending year of the valid date range (e.g., 2023).
        """
        super().__init__(column=column, dqa_column=dqa_column)
        self._start_date = start_date
        self._end_date = end_date

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the task to detect invalid dates and flags entries outside the valid date range.

        This method adds a new column to the DataFrame that flags rows where the date falls
        outside the range of `start_date` and `end_date`.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the date data to be analyzed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column indicating whether the dates are invalid.
        """
        result = data[self._column].parallel_apply(self._date_invalid)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

    def _date_invalid(self, date: np.datetime64) -> bool:
        """
        Checks whether the date is outside the valid date range.

        This method compares the date against the specified `start_date` and `end_date`
        and flags it as invalid if it falls outside this range.

        Args:
        -----
        date : np.datetime64
            The input date to be analyzed.

        Returns:
        --------
        bool
            True if the date is outside the valid range, False otherwise.
        """
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
class DetectProfanityTask(DQATask):
    """Detects profanity in the designated column of a dataframe.

    Note: This class leverages the multiprocessing module. To avoid unintended behavior and
    recursively spawning processes, this should be executed with the __main__ shield. This
    is not strictly required in a WSL2 virtual machine, but should be refactored behind
    a __main__  shield for cross-platform portability.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_has_profanity",
        n_jobs: int = 12,
    ):
        """
        Initializes the DetectProfanityTask with the column names.

        Args:
            column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating
                whether the text contains profanity or not.
            n_jobs (int): Number of cpus corresponding to 'jobs' in a concurrent context.
        """
        super().__init__(column=column, dqa_column=dqa_column)
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
        result = data[self._column].parallel_apply(detect_profanity)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMAIL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(DQATask):
    """
    A data quality assurance (DQA) task that detects email addresses in a specified text column.

    This task inspects a text column in a DataFrame and flags entries that contain email addresses.
    The results are stored in a corresponding DQA column, which indicates whether an email address was detected.

    Attributes:
    -----------
    _column : str
        The name of the column containing the text to be inspected for email addresses.
    _dqa_column : str
        The name of the column where the results (email detection flags) will be stored.

    Methods:
    --------
    run(data: pd.DataFrame) -> pd.DataFrame
        Executes the task to detect email addresses in the specified column and flags entries in the DQA column.
    _contains_email(text: str) -> bool
        A helper method that checks if a given text contains an email address.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "contains_email",
    ):
        """
        Initializes the DetectEmailTask with the specified text column and DQA column.

        Args:
        -----
        column : str, optional
            The name of the column containing the text to be analyzed. Default is "content".
        dqa_column : str, optional
            The name of the column where the results (email detection flags) will be stored.
            Default is "contains_email".
        """
        super().__init__(column=column, dqa_column=dqa_column)

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Executes the task to detect email addresses in the specified text column.

        This method applies the email detection logic to each row in the DataFrame
        and flags rows where email addresses are detected in the text.

        Args:
        -----
        data : pd.DataFrame
            The input DataFrame containing the text data to be analyzed.

        Returns:
        --------
        pd.DataFrame
            The input DataFrame with an additional column indicating whether the text contains an email address.
        """
        result = data[self._column].parallel_apply(self._contains_email)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

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
class DetectURLTask(DQATask):
    """Detects the presence of URLs in a text column.

    Args:
        column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_url",
    ):
        super().__init__()
        self._column = column
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

        result = data[self._column].parallel_apply(self._contains_url)

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

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
class DetectPhoneNumberTask(DQATask):
    """Detects the presence of phone numbers in a text column.

    Args:
        column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_phone_number",
    ):
        super().__init__()
        self._column = column
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

        result = data[self._column].parallel_apply(self._contains_phone_number)
        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result

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
#                               DETECT SPELLING ERRORS                                             #
# ------------------------------------------------------------------------------------------------ #
def is_misspelled(word):
    """
    Checks the spelling of the word and returns True if it is misspelled

    Args:
        word (str): The word to be analyzed.

    Returns:
        bool: True if the word is misspelled. False otherwise.
    """
    suggestions = sym_spell.lookup(word, Verbosity.CLOSEST, max_edit_distance=2)

    if suggestions:
        try:
            print(suggestions[0].distance)
            print(suggestions[0].distance == 0)
            return suggestions[0].distance > 0
        except Exception:
            return False
    else:
        return False


class DetectSpellingErrorsTask(DQATask):
    """Detects the spelling errors in text column.

    Args:
        column (str): Name of column containing text to search.
        dqa_column (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "dqa_spelling_errors",
    ):
        super().__init__(column=column, dqa_column=dqa_column)

    # @cachenow
    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        lol = data[self._column].parallel_apply(
            lambda x: [is_misspelled(word) for word in x.split()]
        )
        result = lol.parallel_apply(lambda x: any(x))

        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result
