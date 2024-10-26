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
# Modified   : Friday October 25th 2024 07:26:42 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
import os
import re
import warnings
from abc import abstractmethod
from typing import Any, Optional

import emoji
import fasttext
import pandas as pd
from lingua import Language, LanguageDetectorBuilder
from pandarallel import pandarallel
from symspellpy import SymSpell

from discover.flow.base.task import Task
from discover.infra.service.cache.cachenow import cachenow
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.visual.print import Printer

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=12, verbose=False)
# ------------------------------------------------------------------------------------------------ #
printer = Printer()
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
    """

    def __init__(
        self, dqa_column: str = None, column: Optional[str] = None, force: bool = False
    ):
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
        self._force = force

    @property
    def column(self) -> str:
        """Returns the name of the column to inspect."""
        return self._column

    @property
    def dqa_column(self) -> str:
        """Returns the name of the column where anomalies are flagged."""
        return self._dqa_column

    @property
    def force(self) -> bool:
        """Whether to ignore the cache and force the task to execute."""
        return self._force

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


# ------------------------------------------------------------------------------------------------ #
#                                       DUPLICATE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateTask(DQATask):
    """
    A task that detects duplicate data by row or cell.

    Rows are identified as identical either by row or a cell value. All identical rows are flagged
    in the result so that downstream cleaning tasks can select data to keep.

    Args:
        dqa_column (str): The name of the column to store the duplicate flag results. The new column will contain boolean values indicating whether the row is a duplicate.
        column (Optional[str]): The column used to detect duplicates. If None, all columns are considered. Defaults to None.
        force (bool): Whether to force execution despite existence of the result in cache.

    Attributes:
        column (Optional[str]):  The column(s) to check for duplicates.
        dqa_column (str): The name of the column where the duplicate flag is stored.
    """

    def __init__(
        self,
        dqa_column: str,
        column: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """
        Initializes the DetectDuplicateTask with the specified column(s) to check for duplicates and the name of the column to store the results.

        Args:
            dqa_column (str): The column name where the duplicate flag will be stored.
            column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) to use for detecting duplicates. If None, the entire row will be checked for duplicates. Defaults to None.
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects duplicate rows based on the specified columns and creates a new column to flag duplicates.

        Args:
            data (pd.DataFrame): The input DataFrame to check for duplicates.

        Returns:
            pd.DataFrame: A DataFrame with a new column indicating whether each row is a duplicate, based on the specified columns.
        """
        if self._column:
            result = data.duplicated(subset=self._column, keep=False)
        else:
            result = data.duplicated(keep=False)

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                            DETECT NON-ENGLISH TASK                                               #
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
        force: bool = False,
    ):
        super().__init__(column=column, dqa_column=dqa_column, force=force)
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_logger
    @cachenow
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
        force: bool = False,
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
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
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
        force: bool = False,
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
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
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
        force: bool = False,
    ):
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
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
        force: bool = False,
    ):
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """Detects phone numbers in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._column].parallel_apply(self._contains_phone_number)
        result = result.rename(self._dqa_column)

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
#                                   DETECT MISSING REVIEWS                                         #
# ------------------------------------------------------------------------------------------------ #
class DetectMissingReviewsTask(DQATask):
    """
    A task that detects zero-length (missing) reviews within a specified column of a DataFrame.

    This task checks the specified column (representing review lengths) for zero-length entries, which indicate
    reviews that contain no content. It creates a new column indicating whether a review is missing (zero-length)
    for each row.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will check for zero-length reviews.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored,
                           indicating whether the review is missing.
    """

    def __init__(
        self,
        column: str = "review_length",
        dqa_column: str = "b_missing_review",
        force: bool = False,
    ):
        """
        Initializes the DetectMissingReviewsTask.

        Args:
            column (str): The name of the DataFrame column that contains the review lengths. Defaults to "review_length".
            dqa_column (str): The name of the output column to store the results. Defaults to "b_missing_review".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect zero-length (missing) reviews in the specified column and stores the result.

        The method checks the specified column for reviews that have a length of 0, indicating missing content.
        The result is a boolean Series where True indicates the presence of a zero-length (missing) review in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows where the review is zero-length and False otherwise.
        """

        result = data[self._column] == 0
        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                   DETECT NON-ASCII CHARS                                         #
# ------------------------------------------------------------------------------------------------ #
class DetectNonAsciiCharsTask(DQATask):
    """
    A task that detects non-ASCII characters within a specified column of a DataFrame.

    This task searches for any characters outside the standard ASCII range (0x00 to 0x7F) in a specified column.
    It creates a new column indicating whether non-ASCII characters were found in each row. The task is run
    in parallel for performance optimization.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will search for non-ASCII characters.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored,
                           indicating whether non-ASCII characters were found.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_non_ascii_chars",
        force: bool = False,
    ):
        """
        Initializes the DetectNonAsciiCharsTask.

        Args:
            column (str): The name of the DataFrame column to check for non-ASCII characters. Defaults to "content".
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_non_ascii_chars".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect non-ASCII characters in the specified column and stores the result.

        The method searches for any non-ASCII characters (those outside the 0x00 to 0x7F range) in the specified
        column of the DataFrame using parallel processing. The result is a boolean Series where True indicates
        the presence of non-ASCII characters in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows that contain non-ASCII characters and False otherwise.
        """

        result = data[self._column].parallel_apply(
            lambda x: bool(re.search(r"[^\x00-\x7F]", str(x)))
        )

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT EXCESSIVE NUMBERS                                         #
# ------------------------------------------------------------------------------------------------ #
class DetectExcessiveNumbersTask(DQATask):
    """
    A task that detects excessive numbers (long sequences of digits) within a specified column of a DataFrame.

    This task searches for sequences of digits that exceed a certain threshold (e.g., 7 or more digits in a row)
    in a specified column and creates a new column indicating whether excessive numbers were found in each row.
    The task is run in parallel for performance optimization.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will search for excessive numbers.
        _threshold (int): The minimum number of consecutive digits to be considered excessive. Defaults to 7.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored,
                           indicating whether excessive numbers were found.
    """

    def __init__(
        self,
        column: str = "content",
        threshold: int = 7,
        dqa_column: str = "b_contains_excessive_numbers",
        force: bool = False,
    ):
        """
        Initializes the DetectExcessiveNumbersTask.

        Args:
            column (str): The name of the DataFrame column to check for excessive numbers. Defaults to "content".
            threshold (int): The minimum number of consecutive digits that is considered excessive. Defaults to 7.
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_excessive_numbers".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)
        self._threshold = threshold

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect excessive numbers in the specified column and stores the result.

        The method searches for sequences of digits that exceed the given threshold (e.g., 7 or more consecutive digits)
        in the specified column of the DataFrame using parallel processing. The result is a boolean Series where True
        indicates the presence of excessive numbers in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows that contain excessive numbers and False otherwise.
        """

        result = data[self._column].parallel_apply(
            lambda x: bool(re.search(r"\d{" + str(self._threshold) + ",}", str(x)))
        )

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                   DETECT CONTROL CHARS                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectControlCharsTask(DQATask):
    """
    A task that detects control characters within a specified column of a DataFrame.

    This task searches for control characters (non-printable ASCII characters) such as null (`\x00`),
    backspace (`\x08`), and delete (`\x7F`) in a specified column. It creates a new column that flags
    rows where control characters are found. The task is run in parallel for performance optimization.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will search for control characters.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored,
                           indicating whether control characters were found.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_control_chars",
        force: bool = False,
    ):
        """
        Initializes the DetectControlCharsTask.

        Args:
            column (str): The name of the DataFrame column to check for control characters. Defaults to "content".
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_control_chars".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect control characters in the specified column and stores the result.

        The method searches for control characters (`\x00-\x1F`, `\x7F`) in the specified column of the DataFrame
        using parallel processing. The result is a boolean Series where True indicates the presence of control characters
        in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows that contain control characters and False otherwise.
        """

        result = data[self._column].parallel_apply(
            lambda x: bool(re.search(r"[\x00-\x1F\x7F]", str(x)))
        )

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                DETECT EXCESSIVE WHITESPACE                                       #
# ------------------------------------------------------------------------------------------------ #
class DetectExcessiveWhitespaceTask(DQATask):
    """
    A task that detects excessive whitespace within a specified column of a DataFrame.

    This task checks for the presence of two or more consecutive whitespace characters in a specified column
    and creates a new column indicating whether excessive whitespace was found in each row. The task is run
    in parallel for performance optimization.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will search for excessive whitespace.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored
                           indicating whether excessive whitespace was found.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_excessive_whitespace",
        force: bool = False,
    ):
        """
        Initializes the DetectExcessiveWhitespaceTask.

        Args:
            column (str): The name of the DataFrame column to check for excessive whitespace. Defaults to "content".
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_excessive_whitespace".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect excessive whitespace in the specified column and stores the result.

        The method searches for two or more consecutive whitespace characters within the specified column
        of the DataFrame using parallel processing. The result is a boolean Series where True indicates
        the presence of excessive whitespace in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows that contain excessive whitespace and False otherwise.
        """

        result = data[self._column].parallel_apply(
            lambda x: bool(re.search(r"\s{2,}", str(x)))
        )

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                                    DETECT HTML CHARS                                             #
# ------------------------------------------------------------------------------------------------ #
class DetectHTMLCharsTask(DQATask):
    """
    A task that detects the presence of HTML tags or HTML entities within a specified column of a DataFrame.

    This task checks for HTML characters (e.g., `<div>`, `&amp;`) in a specified column and creates
    a new column indicating whether HTML characters were found in each row. The task is run in parallel
    for performance optimization.

    Attributes:
        _column (str): The name of the column in the DataFrame where the task will search for HTML characters.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored
                           indicating whether HTML characters were found.
    """

    def __init__(
        self,
        column: str = "content",
        dqa_column: str = "b_contains_HTML_chars",
        force: bool = False,
    ):
        """
        Initializes the DetectHTMLCharsTask.

        Args:
            column (str): The name of the DataFrame column to check for HTML characters. Defaults to "content".
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_HTML_chars".
        """
        super().__init__(column=column, dqa_column=dqa_column, force=force)

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect HTML characters in the specified column and stores the result.

        The method searches for HTML tags (e.g., `<div>`) and HTML entities (e.g., `&amp;`) within the specified
        column of the DataFrame using parallel processing. The result is a boolean Series where True indicates
        the presence of HTML characters in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the column to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows that contain HTML characters and False otherwise.
        """

        result = data[self._column].parallel_apply(
            lambda x: bool(
                re.search(r"<[^>]+>", str(x)) or re.search(r"&[a-zA-Z]+;", str(x))
            )
        )

        result = result.rename(self._dqa_column)

        return result


# ------------------------------------------------------------------------------------------------ #
#                             DETECT INCONSISTENT APP ID/NAME                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectInconsistentIdNamesTask(DQATask):
    """
    A task that detects inconsistencies between ID and name pairs in a DataFrame.

    This task checks for inconsistent relationships between an ID column and a name column. It flags instances
    where the same ID is associated with multiple names or where the same name is associated with multiple IDs.
    The result is a new column that indicates whether inconsistencies were found for each row.

    Attributes:
        _id_column (str): The name of the column in the DataFrame that contains the IDs.
        _name_column (str): The name of the column in the DataFrame that contains the names.
        _dqa_column (str): The name of the output column where the boolean result (True/False) will be stored,
                           indicating whether inconsistencies were found between IDs and names.
    """

    def __init__(
        self,
        id_column: str,
        name_column: str,
        dqa_column: str = "b_contains_inconsistent_id_name",
        force: bool = False,
    ):
        """
        Initializes the DetectInconsistentIdNamesTask.

        Args:
            id_column (str): The name of the DataFrame column that contains the IDs.
            name_column (str): The name of the DataFrame column that contains the names.
            dqa_column (str): The name of the output column to store the results. Defaults to "b_contains_inconsistent_id_name".
        """
        super().__init__(dqa_column=dqa_column, force=force)
        self._id_column = id_column
        self._name_column = name_column

    @task_logger
    @cachenow
    def run(self, data: pd.DataFrame):
        """
        Runs the task to detect inconsistent ID-name pairs in the specified columns and stores the result.

        The method checks for duplicate IDs associated with different names or duplicate names associated with
        different IDs. The result is a boolean Series where True indicates the presence of inconsistencies between
        IDs and names in a row.

        Args:
            data (pd.DataFrame): The input DataFrame containing the ID and name columns to be processed.

        Returns:
            pd.Series: A boolean Series with True for rows where ID-name inconsistencies are found and False otherwise.
        """
        pairs = data[[self._id_column, self._name_column]].drop_duplicates()

        result = pairs.duplicated(
            subset=[self._id_column], keep=False
        ) | pairs.duplicated(subset=[self._name_column], keep=False)

        result = result.rename(self._dqa_column)

        return result
