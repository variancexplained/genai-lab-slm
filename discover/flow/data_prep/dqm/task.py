#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/dqm/task.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Sunday November 10th 2024 09:56:03 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""
import logging
import os
import unicodedata
import warnings
from abc import abstractmethod
from typing import Optional

import fasttext
from lingua import Language, LanguageDetectorBuilder
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"
# ------------------------------------------------------------------------------------------------ #
#                     LANGUAGE MODELS FOR LANGUAGE DETECTION                                       #
# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None  # Suppress FastText warnings
fasttext_model = fasttext.load_model("models/language_detection/lid.176.bin")


# ------------------------------------------------------------------------------------------------ #
#                               DATA QUALITY BASE CLASS                                            #
# ------------------------------------------------------------------------------------------------ #
class DQMTask(Task):

    def __init__(self):
        super().__init__()


# ------------------------------------------------------------------------------------------------ #
#                       DETECT OR REPAIR OR REPAIR BASE CLASS                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairTask(Task):
    """
    A PySpark task class that performs detection or repair operations on a DataFrame
    based on a specified regular expression pattern.

    Attributes:
        _pattern (str): The regular expression pattern to use for detection or repair.
        _column (str): The name of the column in the DataFrame to apply the operation.
        _mode (str): The mode of operation, either 'detect' or 'repair'.
        _new_column (Optional[str]): The name of the new column for detection results.
        _threshold (Optional[float]): The threshold for detection; if set, counts above
            this value will return True.
        _replacement (Optional[str]): The replacement string for repair mode.
        _logger (logging.Logger): The logger used to log messages and errors.
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[float] = None,
        replacement: Optional[str] = None,
    ):
        super().__init__()
        self._pattern = pattern
        self._column = column
        self._mode = mode
        self._new_column = new_column
        self._threshold = threshold
        self._replacement = replacement

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Executes the task in the specified mode ('detect' or 'repair').

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The modified DataFrame after applying detection or repair.

        Raises:
            ValueError: If the mode is invalid (not 'detect' or 'repair').
        """
        if self._mode == "detect":
            return self.detect(data=data)
        elif self._mode == "repair":
            return self.repair(data=data)
        else:
            msg = (
                f"Invalid mode: {self._mode}. Valid values are 'detect', and 'repair'."
            )
            self._logger.error(msg)
            raise ValueError(msg)

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects occurrences of the pattern in the specified column and adds a binary
        column indicating whether the count of matches meets the threshold.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with the new binary column indicating matches.
        """
        if self._threshold:
            data = data.withColumn(
                self._new_column,
                (
                    (
                        F.regexp_count(F.col(self._column), F.lit(self._pattern))
                        / F.size(F.split(F.col(self._column), " "))
                    )
                    > self._threshold
                ).cast("boolean"),
            )
        else:
            data = data.withColumn(
                self._new_column,
                (F.regexp_count(F.col(self._column), F.lit(self._pattern)) > 0).cast(
                    "boolean"
                ),
            )

        return data

    @abstractmethod
    def repair(self, data: DataFrame) -> DataFrame:
        """Replace and Remove subclasses must defined their repair methods."""


# ------------------------------------------------------------------------------------------------ #
#                                DETECT OR REPLACE TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class DetectOrReplaceTask(DetectOrRepairTask):
    """
    A PySpark task class for detecting or replacing text patterns in a DataFrame column.
    This class provides functionality to either flag the presence of a specified pattern
    or replace occurrences of the pattern with a specified replacement string.

    Attributes:
        _pattern (str): The regular expression pattern to detect or replace.
        _column (str): The name of the column to check for the pattern (default is "content").
        _mode (str): The mode of operation, either 'detect' to flag patterns or 'repair' to replace them.
        _new_column (Optional[str]): The name of the new column for detection flags (optional).
        _threshold (Optional[float]): An optional threshold for detection logic (default is None).
        _replacement (Optional[str]): The string to replace matched patterns with in 'repair' mode (optional).
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[float] = None,
        replacement: Optional[str] = None,
    ):
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            threshold=threshold,
        )

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Replaces occurrences of the specified pattern in the column with the replacement string.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with the specified replacements applied.
        """
        return data.withColumn(
            self._column,
            F.regexp_replace(F.col(self._column), self._pattern, self._replacement),
        )


# ------------------------------------------------------------------------------------------------ #
#                                DETECT OR REMOVE TASK                                             #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRemoveTask(DetectOrRepairTask):
    """
    A PySpark task class for detecting or removing rows in a DataFrame based on a specified pattern.
    This class provides functionality to either flag the presence of a pattern or remove rows
    where the pattern is detected.

    Attributes:
        _pattern (str): The regular expression pattern to detect.
        _column (str): The name of the column to check for the pattern (default is "content").
        _mode (str): The mode of operation, either 'detect' to flag patterns or 'repair' to remove rows.
        _new_column (Optional[str]): The name of the new column for detection flags (optional).
        _threshold (Optional[float]): An optional threshold for detection logic (default is None).
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[float] = None,
    ):
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
            threshold=threshold,
        )

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes rows from the DataFrame where the specified pattern is detected.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with offending rows removed based on the detection flag.
        """
        return data.filter(F.col(self._new_column) is False)


# ------------------------------------------------------------------------------------------------ #
#                          DETECT OR REPAIR DUPLICATE REVIEW ID                                    #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairDuplicateReviewIdTask(DetectOrRepairTask):
    """
    A PySpark task class for detecting or repairing duplicate review IDs in a DataFrame.
    The class provides functionality to either flag duplicate IDs in a new column or
    remove duplicate rows based on the specified ID column.

    Attributes:
        _mode (str): The mode of operation, either 'detect' or 'repair'.
        _column (str): The name of the column to check for duplicates (default is "id").
        _new_column (str): The name of the new column for detecting duplicates
            (default is "dqm_duplicate_review_id").
    """

    def __init__(
        self,
        mode: str = "detect",
        column: str = "id",
        new_column: str = "dqm_duplicate_review_id",
    ):
        """
        Initializes the DetectOrRepairDuplicateReviewIdTask class.

        Args:
            mode (str): The mode of operation, either 'detect' or 'repair'.
            column (str): The name of the column to check for duplicates.
            new_column (str): The name of the new column for detecting duplicates.
        """
        super().__init__(column=column, mode=mode, new_column=new_column)

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes duplicate rows from the DataFrame based on the specified column.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with duplicate rows removed.
        """
        # Define a Window spec
        window_spec = Window.partitionBy("id").orderBy(
            F.col("date").desc(),  # Latest review date first
            F.col("review_length").desc(),  # Largest review length first
            F.col("vote_count").desc(),  # Highest vote count first
        )
        # Add a row number column based on the ordering
        data = data.withColumn("row_number", F.row_number().over(window_spec))

        # Keep only the rows with row_number = 1 (i.e., the top-ranked rows)
        data = data.filter(F.col("row_number") == 1).drop("row_number")

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Flags duplicate review IDs in a new column. The new column will contain a boolean
        value indicating whether the row is a duplicate.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with an additional column indicating duplicates.
        """
        # Create a window specification for checking duplicates
        window_spec = Window.partitionBy(self._column)

        # Identify duplicates
        data = data.withColumn(
            self._new_column,
            (F.count(self._column).over(window_spec) > 1).cast("boolean"),
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                             DETECT OR REPAIR URL TASK                                            #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairURLTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing URLs in a DataFrame column.
    The class provides functionality to either flag the presence of URLs in a new
    column or replace URLs with a specified replacement string.

    Attributes:
        _column (str): The name of the column to check for URLs (default is "content").
        _new_column (str): The name of the new column for URL detection flags (default is "dqa_has_url").
        _replacement (str): The string to replace URLs with in 'repair' mode (default is "[URL]").
        _threshold (float): An optional threshold for detection logic (default is None).
        _mode (str): The mode of operation, either 'detect' to flag URLs or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_url",
        replacement: str = "[URL]",
        threshold: float = None,
        mode: str = "detect",
    ) -> None:
        """
        Initializes the DetectOrRepairURLTask with the specified parameters.

        Args:
            column (str): The name of the column to check for URLs.
            new_column (str): The name of the new column for URL detection flags.
            replacement (str): The string to replace URLs with in 'repair' mode.
            threshold (float): An optional threshold for detection logic.
            mode (str): The mode of operation, either 'detect' to flag URLs or 'repair' to replace them.
        """
        pattern = r"(https?:\/\/)?(www\.)?[\w\-_]+(\.[\w\-_]+)+([\/\w\-_\.]*)*"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            threshold=threshold,
        )


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR EMAIL ADDRESS TASK                                    #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairEmailAddressTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing email addresses in a DataFrame column.
    The class provides functionality to either flag the presence of email addresses in a
    new column or replace email addresses with a specified replacement string.

    Attributes:
        _column (str): The name of the column to check for email addresses (default is "content").
        _new_column (str): The name of the new column for email detection flags (default is "dqa_has_email").
        _replacement (str): The string to replace email addresses with in 'repair' mode (default is "[EMAIL]").
        _threshold (float): An optional threshold for detection logic (default is None).
        _mode (str): The mode of operation, either 'detect' to flag email addresses or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_email",
        replacement: str = "[EMAIL]",
        threshold: float = None,
        mode: str = "detect",
    ) -> None:
        pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            threshold=threshold,
        )


# ------------------------------------------------------------------------------------------------ #
#                          DETECT OR REPAIR PHONE NUMBER TASK                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairPhoneNumberTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing phone numbers in a DataFrame column.
    The class provides functionality to either flag the presence of phone numbers in a
    new column or replace phone numbers with a specified replacement string.

    Attributes:
        _column (str): The name of the column to check for phone numbers (default is "content").
        _new_column (str): The name of the new column for phone number detection flags (default is "dqa_has_phone").
        _replacement (str): The string to replace phone numbers with in 'repair' mode (default is "[PHONE]").
        _threshold (float): An optional threshold for detection logic (default is None).
        _mode (str): The mode of operation, either 'detect' to flag phone numbers or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_phone",
        replacement: str = "[PHONE]",
        threshold: float = None,
        mode: str = "detect",
    ) -> None:
        pattern = r"(\+?\d{1,3})?[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            threshold=threshold,
        )


# ------------------------------------------------------------------------------------------------ #
#                       DETECT OR REPAIR EXCESSIVE SPECIAL CHARS TASK                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveSpecialCharsTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or removing rows with excessive special characters in a DataFrame column.
    The class provides functionality to either flag rows that have more special characters than a specified threshold
    or remove those rows from the DataFrame.

    Attributes:
        _column (str): The name of the column to check for excessive special characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_excess_special_chars").
        _threshold (float): The threshold for the number of special characters required to trigger detection (default is None).
        _mode (str): The mode of operation, either 'detect' to flag rows or 'repair' to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_excess_special_chars",
        threshold: float = None,
        mode: str = "detect",
        replacement: str = " ",
    ) -> None:
        pattern = r"[#<>~]"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
            threshold=threshold,
            replacement=replacement,
        )


# ------------------------------------------------------------------------------------------------ #
#                         DETECT OR REPAIR UNICODE TEXT TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonASCIITextTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing text with Unicode characters in a DataFrame column.
    The class provides functionality to either flag rows with non-ASCII (Unicode) characters or
    replace them by normalizing the text to remove these characters.

    Attributes:
        _column (str): The name of the column to check for Unicode characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_unicode_chars").
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_non_ascii_chars",
        mode: str = "detect",
    ) -> None:
        """
        Initializes the DetectOrRepairUnicodeTextTask with the specified column names.

        Args:
            column (str): The name of the column to check for Unicode characters.
            new_column (str): The name of the new column for detection flags.
        """
        pattern = (
            r"[^\x00-\x7F]"  # Regex pattern to match non-ASCII (Unicode) characters
        )
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
        )

    @staticmethod
    def _normalize_text(text):
        """
        Normalizes Unicode text to remove non-ASCII characters.

        Args:
            text (str): The input text to be normalized.

        Returns:
            str: The normalized text with non-ASCII characters removed.
        """
        normalized_text = unicodedata.normalize("NFKD", text)
        ascii_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return ascii_text

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs the DataFrame by normalizing text in the specified column to remove non-ASCII characters.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with normalized text in the specified column.
        """
        # Register the normalization function as a UDF
        normalize_text_udf = udf(self._normalize_text)

        # Apply the UDF to the specified column
        return data.withColumn(self._column, normalize_text_udf(F.col(self._column)))


# ------------------------------------------------------------------------------------------------ #
#                        DETECT OR REPAIR CONTROL CHARS TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairControlCharsTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing control characters in a DataFrame column.
    The class provides functionality to either flag the presence of control characters or
    remove/replace them with a specified replacement string.

    Attributes:
        _column (str): The name of the column to check for control characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_ctrl_chars").
        _replacement (str): The string to replace control characters with in 'repair' mode (default is an empty string).
        _mode (str): The mode of operation, either 'detect' to flag control characters or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_ctrl_chars",
        replacement: str = "",
        mode: str = "detect",
    ) -> None:
        """
        Initializes the DetectOrRepairControlCharsTask with the specified parameters.

        Args:
            column (str): The name of the column to check for control characters.
            new_column (str): The name of the new column for detection flags.
            replacement (str): The string to replace control characters with in 'repair' mode.
            mode (str): The mode of operation, either 'detect' to flag control characters or 'repair' to replace them.
        """
        pattern = r"[\x00-\x1F\x7F]"  # Regex pattern to match control characters
        super().__init__(
            pattern=pattern,
            column=column,
            mode=mode,
            replacement=replacement,
            new_column=new_column,
        )


# ------------------------------------------------------------------------------------------------ #
#                       DETECT OR REPAIR HTML CHARS TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairHTMLCharsTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing HTML character entities in a DataFrame column.
    The class provides functionality to either flag the presence of HTML character entities
    or remove/replace them with a specified replacement string.

    Attributes:
        _column (str): The name of the column to check for HTML character entities (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_html_chars").
        _mode (str): The mode of operation, either 'detect' to flag HTML character entities or 'repair' to replace them.
        _replacement (str): The string to replace HTML character entities with in 'repair' mode (default is an empty string).
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_html_chars",
        mode: str = "detect",
        replacement: str = "",
    ) -> None:
        pattern = r"&[#A-Za-z0-9]+;"  # Regex pattern to match HTML character entities
        super().__init__(
            pattern=pattern,
            column=column,
            mode=mode,
            replacement=replacement,
            new_column=new_column,
        )


# ------------------------------------------------------------------------------------------------ #
#                     DETECT OR REPAIR EXCESSIVE WHITESPACE TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveWhitespaceTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing excessive whitespace in a DataFrame column.
    The class provides functionality to either flag rows with excessive whitespace or
    replace multiple whitespace characters with a single space.

    Attributes:
        _column (str): The name of the column to check for excessive whitespace (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_excess_whitespace").
        _mode (str): The mode of operation, either 'detect' to flag excessive whitespace or 'repair' to replace it.
        _replacement (str): The string to replace excessive whitespace with in 'repair' mode (default is a single space).
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_excess_whitespace",
        mode: str = "detect",
        replacement: str = " ",
    ) -> None:
        pattern = r"\s{2,}"  # Regex pattern to match two or more consecutive whitespace characters
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
            replacement=replacement,
        )


# ------------------------------------------------------------------------------------------------ #
#                         DETECT OR REPAIR ACCENTED CHARS TASK                                     #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairAccentedCharsTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing accented and diacritic characters in a DataFrame column.
    The class provides functionality to either flag the presence of accented characters or
    normalize text by removing accents and diacritics.

    Attributes:
        _column (str): The name of the column to check for accented characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_accented_chars").
        _mode (str): The mode of operation, either 'detect' to flag accented characters or 'repair' to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_accented_chars",
        mode: str = "detect",
    ) -> None:
        # Regex pattern for accented and diacritic characters
        pattern = r"[\u00C0-\u024F]"
        super().__init__(
            pattern=pattern, column=column, new_column=new_column, mode=mode
        )

    @staticmethod
    def _remove_accents(text):
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

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs the DataFrame by normalizing text in the specified column to remove accented characters.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with normalized text in the specified column.
        """
        # Register the normalization function as a UDF
        remove_accents_udf = udf(self._remove_accents)

        # Apply the UDF to the specified column
        return data.withColumn(self._column, remove_accents_udf(F.col(self._column)))


# ------------------------------------------------------------------------------------------------ #
#                      DETECT OR REPAIR NON-ENGLISH TASK                                               #
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
class DetectOrRepairNonEnglishTask(DetectOrRemoveTask):
    """
    A PySpark task class for detecting or removing non-English text in a DataFrame column.
    The class provides functionality to either flag non-English text using FastText and Lingua
    language detection libraries or remove rows where the text is non-English.

    Attributes:
        _column (str): The name of the column to check for non-English text (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_non_english").
        _mode (str): The mode of operation, either 'detect' to flag non-English text or 'remove' to delete such rows.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "dqa_has_non_english",
        mode: str = "detect",
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column

    def _run_fasttext(self, text: str) -> bool:
        """
        Primary language detection using FastText.

        Args:
            text (str): The input text to be checked for language.

        Returns:
            bool: True if the text is non-English, False otherwise.
        """
        # Placeholder for FastText language detection logic
        return lm_fasttext(text)

    def _run_lingua(self, text: str) -> bool:
        """
        Secondary language detection using Lingua.

        Args:
            text (str): The input text to be checked for language.

        Returns:
            bool: True if the text is non-English, False otherwise.
        """
        # Placeholder for Lingua language detection logic
        return lm_lingua(text)

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects non-English text in the specified column using FastText and Lingua.

        Args:
            data (DataFrame): A PySpark DataFrame containing the data to be processed.

        Returns:
            DataFrame: A PySpark DataFrame with an additional boolean column indicating
            whether the text is non-English.
        """
        # Define UDFs for FastText and Lingua detection
        fasttext_udf = F.udf(self._run_fasttext, BooleanType())
        lingua_udf = F.udf(self._run_lingua, BooleanType())

        # Apply the FastText language detection
        data = data.withColumn(self._new_column, fasttext_udf(F.col(self._column)))

        # Apply the Lingua language detection only to rows flagged as non-English by FastText
        data = data.withColumn(
            self._new_column,
            F.when(
                F.col(self._new_column),  # If FastText marked as non-English
                lingua_udf(
                    F.col(self._column)
                ),  # Run Lingua and check if it also returns True
            ).otherwise(
                False
            ),  # Otherwise, set to False
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR EXCESSIVE ELONGATION                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairElongationTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing elongated characters in a DataFrame column.
    The class provides functionality to either flag instances of character elongation (e.g., "sooo good")
    or replace them by limiting the repetition of characters to a specified maximum.

    Attributes:
        _column (str): The name of the column to check for character elongation (default is "content").
        _threshold (int): The minimum number of consecutive repeating characters to be considered elongation (default is 4).
        _new_column (str): The name of the new column for detection flags (default is "dqa_has_elongation").
        _max_elongation (int): The maximum allowed repetition of characters when repairing (default is 3).
        _mode (str): The mode of operation, either 'detect' to flag elongation or 'repair' to limit character repetition.
    """

    def __init__(
        self,
        column: str = "content",
        threshold: int = 4,
        new_column: str = "dqa_has_elongation",
        max_elongation: int = 3,
        mode: str = "detect",
    ) -> None:
        pattern = rf"(.)\1{{{threshold - 1},}}"  # Regex to detect elongated characters
        replacement = r"\1" * max_elongation  # Replacement pattern to limit elongation
        super().__init__(
            pattern=pattern,
            replacement=replacement,
            column=column,
            new_column=new_column,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR OUTLIERS                                              #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairOutliersTask(DetectOrRemoveTask):
    """
    A PySpark task class for detecting or removing outliers in a DataFrame column.
    The class provides functionality to either flag outliers based on the Interquartile Range (IQR) method
    or remove rows containing outliers.

    Attributes:
        _column (str): The name of the column to check for outliers (default is "vote_count").
        _mode (str): The mode of operation, either 'detect' to flag outliers or 'remove' to delete such rows.
        _new_column (str): The name of the new column for outlier detection flags, dynamically generated based on the column name.
    """

    def __init__(self, column: str = "vote_count", mode: str = "detect") -> None:
        pattern = None
        new_column = f"dqa_outlier_{column}"
        super().__init__(
            pattern=pattern, column=column, mode=mode, new_column=new_column
        )

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects outliers in the specified column using the Interquartile Range (IQR) method.

        Args:
            data (DataFrame): A PySpark DataFrame containing the data to be processed.

        Returns:
            DataFrame: A PySpark DataFrame with an additional boolean column indicating
            whether each row is an outlier.
        """
        # Compute Q1, Q3, and IQR
        quantiles = data.approxQuantile(self._column, [0.25, 0.75], 0.01)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Define outlier detection logic
        is_outlier = F.when(
            (F.col(self._column) < lower_bound) | (F.col(self._column) > upper_bound),
            F.lit(True),
        ).otherwise(F.lit(False))

        # Add the outlier indicator column
        data = data.withColumn(self._new_column, is_outlier)

        return data


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR OUTLIERS                                              #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRemoveShortReviewsTask(DetectOrRemoveTask):
    """
    A PySpark task class for detecting or removing outliers in a DataFrame column.
    The class provides functionality to either flag outliers based on the Interquartile Range (IQR) method
    or remove rows containing outliers.

    Attributes:
        _column (str): The name of the column to check for outliers (default is "vote_count").
        _mode (str): The mode of operation, either 'detect' to flag outliers or 'remove' to delete such rows.
        _new_column (str): The name of the new column for outlier detection flags, dynamically generated based on the column name.
    """

    def __init__(
        self,
        column: str = "review_length",
        mode: str = "detect",
        threshold: int = 3,
        new_column: str = "dqa_short_review",
    ) -> None:
        super().__init__(
            column=column, mode=mode, new_column=new_column, threshold=threshold
        )

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects outliers in the specified column using the Interquartile Range (IQR) method.

        Args:
            data (DataFrame): A PySpark DataFrame containing the data to be processed.

        Returns:
            DataFrame: A PySpark DataFrame with an additional boolean column indicating
            whether each row is an outlier.
        """
        data = data.withColumn(
            self._new_column, (F.col(self._column) < self._threshold).cast("boolean")
        )

        return data
