#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/clean/task.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Monday November 11th 2024 02:31:13 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import os
import re
import unicodedata
import warnings

import fasttext
import pandas as pd
from lingua import Language, LanguageDetectorBuilder
from pandarallel import pandarallel

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(nb_workers=18, verbose=False)

# ------------------------------------------------------------------------------------------------ #
#                     LANGUAGE MODELS FOR LANGUAGE DETECTION                                       #
# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None  # Suppress FastText warnings
fasttext_model = fasttext.load_model("models/language_detection/lid.176.bin")


# ------------------------------------------------------------------------------------------------ #
#                                    Data Cleaning Task                                            #
# ------------------------------------------------------------------------------------------------ #
class DataCleaningTask(Task):
    """
    A task that performs data cleaning by removing rows marked for exclusion and
    summarizing the effects of cleaning operations.

    Attributes:
        _summary (str): A summary of the task execution, describing rows removed or modified.

    Methods:
        summary() -> str:
            Returns a string summary of the task's execution.

        summarize(a: pd.DataFrame, b: pd.DataFrame) -> None:
            Compares DataFrames `a` and `b`, setting the `_summary` attribute to describe
            the difference between them in terms of rows removed or cells modified.

        remove(df: pd.DataFrame) -> pd.DataFrame:
            Removes rows marked for exclusion in the 'exclude' column of the provided
            DataFrame `df`, and drops the 'exclude' column.
    """

    def __init__(self):
        super().__init__()
        self._summary = None

    @property
    def summary(self) -> str:
        """Returns a string summary of the task's execution."""
        return self._summary

    def summarize(self, a: pd.DataFrame, b: pd.DataFrame) -> None:
        """
        Summarizes the difference between two DataFrames `a` and `b` and sets the `_summary` attribute.

        If `a` and `b` are equal, `_summary` is set to None.
        If `a` has more rows than `b`, `_summary` reports the number of rows removed.
        If `a` has fewer rows than `b`, `_summary` reports the number of rows removed.
        If `a` and `b` differ in values, `_summary` reports the number of modified cells.

        Args:
            a (pd.DataFrame): The original DataFrame.
            b (pd.DataFrame): The cleaned DataFrame to compare against.
        """
        self._summary = "No changes made."
        if a.shape[0] > b.shape[0]:
            self._summary = f"Removed {a.shape[0] - b.shape[0]} rows."
        elif a.shape[0] < b.shape[0]:
            self._summary = f"Removed {b.shape[0] - a.shape[0]} rows."
        elif a.eq(b).sum().sum() > 0:
            changed_cells = a.shape[0] * a.shape[1] - a.eq(b).sum().sum()
            self._summary = f"Modified {changed_cells} cells."

    def remove(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows marked for exclusion and drops the 'exclude' column.

        Checks for the presence of an 'exclude' column in the DataFrame `df`. If found,
        it filters out rows where 'exclude' is True and drops the 'exclude' column.

        Args:
            df (pd.DataFrame): The DataFrame to process.

        Returns:
            pd.DataFrame: The cleaned DataFrame with excluded rows removed.
        """
        if "exclude" in df.columns:
            df = df.loc[~df["exclude"]]
            df = df.drop(columns=["exclude"])
        return df


# ------------------------------------------------------------------------------------------------ #
#                                REMOVE DUPLICATE REVIEW ID                                        #
# ------------------------------------------------------------------------------------------------ #
class RemoveDuplicateReviewIdTask(DataCleaningTask):
    """
    A data cleaning task to remove duplicate review entries based on the 'id' column.

    This task sorts the dataset by the 'date' column in ascending order, ensuring the
    latest review (or, if dates are identical, the longest review by position) for each
    unique 'id' is retained. Duplicate entries are identified based on the 'id' column,
    and only the last occurrence of each duplicate is kept. The task maintains the most
    recent version of each review while minimizing redundancy.

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Executes the duplicate removal process, logging execution details, and returns
            a DataFrame with unique 'id' entries.

    Attributes:
        None
    """

    def __init__(self, column: str = "id", sort_by: str = "date", keep: str = "last"):
        super().__init__()
        self._column = column
        self._sort_by = sort_by
        self._keep = keep
        self._summary = None

    @property
    def summary(self) -> str:
        return self._summary

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes duplicate rows based on the 'id' column, keeping only the latest review
        for each unique 'id'.

        Args:
            data (pd.DataFrame): The input DataFrame containing reviews with potential
                duplicate 'id' entries.

        Returns:
            pd.DataFrame: A DataFrame with duplicates removed, containing only the latest
                or most relevant review for each unique 'id'.
        """

        df = data.sort_values(by=self._sort_by, ascending=True)
        df["exclude"] = df.duplicated(subset=self._column, keep=self._keep)
        df = self.remove(df=df)
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                                      MASK URL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class URLMaskTask(DataCleaningTask):
    """
    A data cleaning task to mask URLs within a specified column in a DataFrame.

    This task identifies URLs in text data based on a regular expression pattern and replaces
    them with a specified placeholder string (default: "[URL]"). It operates on a specified
    column and supports efficient application over large datasets.

    Attributes:
        _replacement (str): The string to replace URLs with. Defaults to "[URL]".
        _column (str): The column in which URLs will be masked. Defaults to "content".
        _pattern (str): Regular expression pattern for identifying URLs with or without
            "http", "https", or "www".

    Args:
        replacement (str): The placeholder string to replace URLs with. Defaults to "[URL]".
        column (str): The name of the column in which URLs should be masked. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces URLs in the specified column with the placeholder string and returns
            the updated DataFrame.
    """

    def __init__(self, replacement: str = "[URL]", column: str = "content") -> None:
        """
        Initializes URLMaskTask with specified replacement text and target column.
        """
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"(https?:\/\/)?(www\.)?[\w\-_]+(\.[\w\-_]+)+([\/\w\-_\.]*)*"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Masks URLs in the specified column of the input DataFrame.

        Replaces all URL occurrences within the specified column using the defined regex
        pattern and replacement text. Uses parallel processing for efficiency in large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing the text data with potential URLs.

        Returns:
            pd.DataFrame: Updated DataFrame with URLs masked in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                                 MASK EMAIL ADDRESS TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class EmailAddressMaskTask(DataCleaningTask):
    """
    A data cleaning task to mask email addresses within a specified column in a DataFrame.

    This task detects email addresses based on a regular expression pattern and replaces
    them with a specified placeholder string (default: "[EMAIL]"). It operates on a
    specified column and uses parallel processing for efficiency with large datasets.

    Attributes:
        _replacement (str): The string used to replace email addresses. Defaults to "[EMAIL]".
        _column (str): The column in which email addresses will be masked. Defaults to "content".
        _pattern (str): Regular expression pattern for detecting email addresses.

    Args:
        replacement (str): The placeholder string to replace email addresses with. Defaults to "[EMAIL]".
        column (str): The name of the column in which email addresses should be masked. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces email addresses in the specified column with the placeholder string and
            returns the updated DataFrame.
    """

    def __init__(self, replacement: str = "[EMAIL]", column: str = "content") -> None:
        """
        Initializes EmailAddressMaskTask with specified replacement text and target column.
        """
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Masks email addresses in the specified column of the input DataFrame.

        Replaces all occurrences of email addresses within the specified column using
        the defined regex pattern and replacement text. Utilizes parallel processing
        for efficiency with large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing the text data with potential email addresses.

        Returns:
            pd.DataFrame: Updated DataFrame with email addresses masked in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                                 MASK PHONE NUMBER TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class PhoneNumberMaskTask(DataCleaningTask):
    """
    A data cleaning task to mask phone numbers within a specified column in a DataFrame.

    This task detects phone numbers based on a regular expression pattern and replaces
    them with a specified placeholder string (default: "[PHONE]"). It operates on a
    specified column and uses parallel processing to handle large datasets efficiently.

    Attributes:
        _replacement (str): The placeholder string used to replace phone numbers. Defaults to "[PHONE]".
        _column (str): The column in which phone numbers will be masked. Defaults to "content".
        _pattern (str): Regular expression pattern for detecting phone numbers in various formats.

    Args:
        replacement (str): The placeholder string to replace phone numbers with. Defaults to "[PHONE]".
        column (str): The name of the column in which phone numbers should be masked. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces phone numbers in the specified column with the placeholder string and
            returns the updated DataFrame.
    """

    def __init__(self, replacement: str = "[PHONE]", column: str = "content") -> None:
        """
        Initializes PhoneNumberMaskTask with specified replacement text and target column.
        """
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"(\+?\d{1,3})?[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Masks phone numbers in the specified column of the input DataFrame.

        Replaces all occurrences of phone numbers within the specified column using
        the defined regex pattern and replacement text. Utilizes parallel processing
        for efficient handling of large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential phone numbers.

        Returns:
            pd.DataFrame: Updated DataFrame with phone numbers masked in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                             REMOVE SPECIAL CHARS TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class RemoveExcessiveSpecialCharsTask(DataCleaningTask):
    """"""

    def __init__(self, threshold: float = 0.3, column: str = "content") -> None:
        """
        Initializes RemoveSpecialCharsTask with specified replacement text and target column.
        """
        super().__init__()
        self._threshold = threshold
        self._column = column
        self._pattern = r"[^\w\s]"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes or replaces specified special characters in the given column of the input DataFrame.

        Uses a regex pattern to detect special characters in the specified column, then
        replaces them with the designated replacement string. Supports parallel processing
        for efficiency on large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential special characters.

        Returns:
            pd.DataFrame: Updated DataFrame with specified special characters removed or replaced in the specified column.
        """
        df = data.copy()
        df["exclude"] = df[self._column].parallel_apply(
            lambda text: (
                len(re.findall(self._pattern, text)) / len(text) if len(text) > 0 else 0
            )
            > self._threshold
        )

        df = self.remove(df=df)
        self.summarize(a=data, b=df)
        return df

    def _normalize_text(self, text):
        """Normalizes Unicode text to Compatibility Decomposition Form"""
        normalized_text = unicodedata.normalize("NFKD", text)
        # Encode to ASCII, ignoring characters that can’t be converted to ASCII
        ascii_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return ascii_text


# ------------------------------------------------------------------------------------------------ #
#                             NORMALIZE UNICODE TEXT TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class NormalizeUnicodeTextTask(DataCleaningTask):
    """A task to normalize Unicode text in a specified column of a DataFrame.

    This task normalizes Unicode text using Compatibility Decomposition Form (NFKD)
    and converts it to ASCII, removing characters that cannot be converted.

    Attributes:
        column (str): The name of the column to be normalized. Defaults to "content".
    """

    def __init__(self, column: str = "content") -> None:
        """Initializes NormalizeUnicodeTextTask with the specified column name.

        Args:
            column (str): The name of the column to be normalized. Defaults to "content".
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Runs the normalization task on the specified column of the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing the text to be normalized.

        Returns:
            pd.DataFrame: A DataFrame with the normalized text in the specified column.
        """
        df = data.copy()
        df[self._column] = data[self._column].parallel_apply(self._normalize_text)

        self.summarize(a=data, b=df)
        return df

    def _normalize_text(self, text):
        """Normalizes Unicode text to Compatibility Decomposition Form (NFKD).

        Args:
            text (str): The input text to be normalized.

        Returns:
            str: The normalized ASCII text.
        """
        normalized_text = unicodedata.normalize("NFKD", text)
        # Encode to ASCII, ignoring characters that can’t be converted to ASCII
        ascii_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return ascii_text


# ------------------------------------------------------------------------------------------------ #
#                             REMOVE CONTROL CHARS TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class RemoveControlCharsTask(DataCleaningTask):
    """
    A data cleaning task to remove or replace control characters within a specified column in a DataFrame.

    This task identifies control characters (non-printable characters in the ASCII range,
    such as null, backspace, and delete) in the specified column and replaces them with
    a designated placeholder or removes them entirely if no replacement is provided.
    Removing control characters ensures text is clean and free from non-printable elements
    that can interfere with processing and analysis.

    Attributes:
        _replacement (str): The string used to replace control characters. Defaults to an empty string, which removes them.
        _column (str): The column in which control characters will be removed or replaced. Defaults to "content".
        _pattern (str): Regular expression pattern to detect control characters in the ASCII range.

    Args:
        replacement (str): The placeholder string to replace control characters with. Defaults to an empty string.
        column (str): The name of the column in which control characters should be removed or replaced. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces or removes control characters in the specified column and returns the updated DataFrame.
    """

    def __init__(self, replacement: str = "", column: str = "content") -> None:
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"[\x00-\x1F\x7F]"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes or replaces control characters in the specified column of the input DataFrame.

        Uses a regex pattern to detect control characters in the specified column, replacing
        them with the designated replacement string. Supports parallel processing for efficient
        handling of large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential control characters.

        Returns:
            pd.DataFrame: Updated DataFrame with control characters removed or replaced in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                             REMOVE HTML CHARS TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class RemoveHTMLCharsTask(DataCleaningTask):
    """
    A data cleaning task to remove or replace HTML character entities within a specified column in a DataFrame.

    This task identifies HTML character entities (e.g., `&amp;`, `&#39;`, `&lt;`) and replaces
    them with a specified placeholder or removes them entirely if no replacement is provided.
    The task uses a regular expression to match HTML entities and is designed for text data that
    may contain encoded HTML symbols.

    Attributes:
        _replacement (str): The string used to replace HTML entities. Defaults to an empty string, which removes them.
        _column (str): The name of the column in which HTML entities will be removed or replaced. Defaults to "content".
        _pattern (str): Regular expression pattern to detect HTML character entities.

    Args:
        replacement (str): The placeholder string to replace HTML entities with. Defaults to an empty string.
        column (str): The name of the column in which HTML entities should be removed or replaced. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces or removes HTML entities in the specified column and returns the updated DataFrame.
    """

    def __init__(self, replacement: str = "", column: str = "content") -> None:
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"&[#A-Za-z0-9]+;"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes or replaces HTML entities in the specified column of the input DataFrame.

        Uses a regex pattern to detect HTML character entities in the specified column,
        replacing them with the designated replacement string. Supports parallel processing
        for efficiency with large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential HTML entities.

        Returns:
            pd.DataFrame: Updated DataFrame with HTML entities removed or replaced in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                           REMOVE EXCESSIVE WHITESPACE TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class RemoveExcessiveWhitespaceTask(DataCleaningTask):
    """
    A data cleaning task to normalize excessive whitespace within a specified column in a DataFrame.

    This task identifies occurrences of two or more consecutive whitespace characters and
    replaces them with a single space or a specified replacement string. It ensures a consistent
    text format by reducing unnecessary whitespace that may interfere with tokenization or analysis.

    Attributes:
        _replacement (str): The string used to replace excessive whitespace. Defaults to a single space.
        _column (str): The column in which excessive whitespace will be normalized. Defaults to "content".
        _pattern (str): Regular expression pattern to detect two or more consecutive whitespace characters.

    Args:
        replacement (str): The string to replace excessive whitespace with. Defaults to a single space.
        column (str): The name of the column in which excessive whitespace should be normalized. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces occurrences of excessive whitespace in the specified column and returns the updated DataFrame.
    """

    def __init__(self, replacement: str = " ", column: str = "content") -> None:
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"\s{2,}"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizes excessive whitespace in the specified column of the input DataFrame.

        Uses a regex pattern to identify sequences of two or more whitespace characters in the
        specified column and replaces them with the designated replacement string. Supports
        parallel processing for efficient handling of large datasets.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential excessive whitespace.

        Returns:
            pd.DataFrame: Updated DataFrame with excessive whitespace normalized in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                            REMOVE NON-ASCII CHARS TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class RemoveNonASCIICharsTask(DataCleaningTask):
    """
    A data cleaning task to remove or replace non-ASCII characters within a specified column in a DataFrame.

    This task identifies non-ASCII characters in the specified column and replaces them
    with a specified placeholder or removes them entirely if no replacement is provided.
    It uses a regular expression to match characters outside the ASCII range, ensuring
    only ASCII-compatible content remains.

    Attributes:
        _replacement (str): The string used to replace non-ASCII characters. Defaults to an empty string, removing them.
        _column (str): The name of the column in which non-ASCII characters will be removed or replaced. Defaults to "content".
        _pattern (str): Regular expression pattern to detect non-ASCII characters.

    Args:
        replacement (str): The placeholder string to replace non-ASCII characters with. Defaults to an empty string, which removes them.
        column (str): The name of the column in which non-ASCII characters should be removed or replaced. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Replaces or removes non-ASCII characters in the specified column and returns the updated DataFrame.
    """

    def __init__(self, replacement: str = "", column: str = "content") -> None:
        """
        Initializes RemoveNonASCIICharsTask with a specified replacement text and target column.

        Args:
            replacement (str): The placeholder string to replace non-ASCII characters with. Defaults to an empty string.
            column (str): The name of the column in which non-ASCII characters should be removed or replaced. Defaults to "content".
        """
        super().__init__()
        self._replacement = replacement
        self._column = column
        self._pattern = r"[^\x00-\x7F]"

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes or replaces non-ASCII characters in the specified column of the input DataFrame.

        Uses a regex pattern to detect non-ASCII characters in the specified column and replaces
        them with the designated replacement string. Supports parallel processing for handling
        large datasets efficiently.

        Args:
            data (pd.DataFrame): Input DataFrame containing text data with potential non-ASCII characters.

        Returns:
            pd.DataFrame: Updated DataFrame with non-ASCII characters removed or replaced in the specified column.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df


# ------------------------------------------------------------------------------------------------ #
#                                 REMOVE ACCENTS TASK                                              #
# ------------------------------------------------------------------------------------------------ #
class RemoveAccentsTask(DataCleaningTask):
    """
    A data cleaning task to normalize accents and diacritics in a specified column of a DataFrame.

    This task removes accents and diacritics from characters, converting them to their base forms
    (e.g., 'é' to 'e') to reduce unnecessary variation in the text.

    Attributes:
        _column (str): The name of the column in which accents and diacritics will be removed.
                       Defaults to "content".

    Args:
        column (str): The column in which accents and diacritics should be normalized. Defaults to "content".

    Methods:
        run(data: pd.DataFrame) -> pd.DataFrame:
            Applies the accent removal transformation to the specified column and returns the updated DataFrame.
    """

    def __init__(self, column: str = "content") -> None:
        """
        Initializes the RemoveAccentsTask with a specified target column.

        Args:
            column (str): The column in which accents and diacritics should be normalized. Defaults to "content".
        """
        super().__init__()
        self._column = column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes accents and diacritics from the specified column in the input DataFrame.

        Args:
            data (pd.DataFrame): The DataFrame containing the text data with accents or diacritics.

        Returns:
            pd.DataFrame: The updated DataFrame with accents and diacritics removed in the specified column.
        """
        df = data.copy()
        # Apply the accent removal function
        df[self._column] = data[self._column].parallel_apply(self._remove_accents)

        self.summarize(a=data, b=df)
        return df

    def _remove_accents(self, text):
        """
        Removes accents and diacritics from a given text string.

        Args:
            text (str): The input string with potential accented characters.

        Returns:
            str: The text with accents and diacritics removed.
        """
        # Decompose characters into base and diacritics, then remove diacritics
        text_normalized = unicodedata.normalize("NFD", text)
        text_without_accents = "".join(
            char for char in text_normalized if unicodedata.category(char) != "Mn"
        )
        return text_without_accents


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
class RemoveNonEnglishTask(DataCleaningTask):
    """Detects non-English text.

    Args:
        column (str): Name of the column containing text to be analyzed.
        new_column_name (str): Name of the new column to be created indicating whether the text is in English or not.
    """

    def __init__(
        self,
        column: str = "content",
        n_jobs: int = 12,
    ):
        super().__init__()
        self._column = column
        self._n_jobs = n_jobs

        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        df = self._run_both(data=data)
        self.summarize(a=data, b=df)
        return df

    def _run_lingua(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        result = data[self._column].parallel_apply(lm_lingua)

        result = result.rename(self._dc_column)

        return result

    def _run_fasttext(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        result = data[self._column].parallel_apply(lm_fasttext)

        result = result.rename(self._dc_column)

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
        df["exclude"] = data[self._column].parallel_apply(lm_fasttext)

        # Apply re-evaluation only to rows where 'is_non_english' is True
        df.loc[df["exclude"], "exclude"] = df.loc[
            df["exclude"], self._column
        ].parallel_apply(lambda text: lm_lingua(text))

        return self.remove(df=df)


# ------------------------------------------------------------------------------------------------ #
#                                 REMOVE EXCESSIVE ELONGATION                                      #
# ------------------------------------------------------------------------------------------------ #
class DelongationTask(DataCleaningTask):
    """
    A data cleaning task to normalize excessive elongation in text by limiting repeated
    characters to a specified maximum.

    Args:
        column (str): The name of the column containing text to process. Default is "content".
        threshold (int): The minimum number of consecutive repeated characters to trigger
            normalization. Default is 4, meaning any character repeated 4 or more times
            will be shortened.
        max_elongation (int): The maximum number of consecutive repeated characters allowed
            after normalization. Default is 3.
    """

    def __init__(
        self, column: str = "content", threshold: int = 4, max_elongation: int = 3
    ) -> None:
        """
        Initializes the DelongationTask with column, threshold, and max_elongation settings.

        Args:
            column (str): The name of the text column to clean.
            threshold (int): The minimum repetition count to start limiting elongation.
            max_elongation (int): The maximum allowed repetitions after normalization.
        """
        super().__init__()
        self._column = column
        self._max_elongation = max_elongation
        # Define the pattern and replacement dynamically based on the parameters
        self._pattern = rf"(.)\1{{{threshold - 1},}}"
        self._replacement = r"\1" * max_elongation

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies the elongation normalization task to the specified column in the DataFrame.

        Args:
            data (pd.DataFrame): The DataFrame containing the text column to process.

        Returns:
            pd.DataFrame: A new DataFrame with the specified column processed to limit excessive
            elongation in words.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.

        Notes:
            This method copies the DataFrame before making changes and logs the task
            execution using the `task_logger` decorator.
        """
        df = data.copy()
        # Apply the replacement using regex
        df[self._column] = data[self._column].parallel_apply(
            lambda x: re.sub(self._pattern, self._replacement, str(x))
        )
        self.summarize(a=data, b=df)
        return df
