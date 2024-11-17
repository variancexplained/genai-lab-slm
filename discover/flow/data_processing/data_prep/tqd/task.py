#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_processing/data_prep/tqd/task.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Sunday November 17th 2024 02:41:53 am                                               #
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
#                       DETECT OR REPAIR OR REPAIR BASE CLASS                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairTask(Task):
    """
    A task for detecting or repairing patterns in text data using specified thresholds.

    This class allows for detection or repair of repeated patterns in text data,
    based on a regular expression pattern and various thresholds. The mode of operation
    can be set to 'detect' or 'repair', depending on the desired functionality.

    Args:

        column (str): The name of the column to process.
        mode (str): The operation mode, either 'detect' or 'repair'.
        pattern (Optional[str]): The regular expression pattern used for detection.
        new_column (Optional[str]): The name of the new column for detection results.
        threshold (Optional[int]): The raw count threshold for detection.
        threshold_word_prop (Optional[float]): The proportion of words threshold for detection.
        threshold_char_prop (Optional[float]): The proportion of characters threshold for detection.
        replacement (Optional[str]): The replacement text used in 'repair' mode.
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[int] = None,
        threshold_word_prop: Optional[float] = None,
        threshold_char_prop: Optional[float] = None,
        replacement: Optional[str] = None,
    ):
        super().__init__()
        self._pattern = pattern
        self._column = column
        self._mode = mode
        self._new_column = new_column
        self._threshold = threshold
        self._threshold_word_prop = threshold_word_prop
        self._threshold_char_prop = threshold_char_prop
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
        # Update the new column to be prefixed by the stage id assigned at runtime.
        self._new_column = f"{self.stage.id}_{self._new_column}"

        # Execution mode
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
        Detects patterns in the text data based on the specified thresholds.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: The DataFrame with a new column indicating the presence
                       of patterns based on the chosen threshold.
        """
        if self._threshold:
            # Handling raw count threshold
            data = data.withColumn(
                self._new_column,
                (
                    F.regexp_count(F.col(self._column), F.lit(self._pattern))
                    >= self._threshold
                ).cast("boolean"),
            )

        elif self._threshold_word_prop:
            # Handling proportion of words threshold
            data = data.withColumn(
                self._new_column,
                (
                    (
                        F.regexp_count(F.col(self._column), F.lit(self._pattern))
                        / F.size(F.split(F.col(self._column), " "))
                    )
                    > self._threshold_word_prop
                ).cast("boolean"),
            )

        elif self._threshold_char_prop:
            # Handling proportion of characters threshold
            data = data.withColumn(
                self._new_column,
                (
                    (
                        F.regexp_count(F.col(self._column), F.lit(self._pattern))
                        / F.length(F.col(self._column))
                    )
                    > self._threshold_char_prop
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
    A task for detecting or replacing patterns in text data.

    This class extends `DetectOrRepairTask` to provide functionality for either
    detecting the presence of patterns or replacing them with specified text.
    It uses regular expressions and specified thresholds to perform detection
    or replacement operations on the text data.

    Args:

        column (str): The name of the column to process.
        mode (str): The operation mode, either 'detect' for flagging patterns or 'replace' to substitute them.
        pattern (Optional[str]): The regular expression pattern used for detection or replacement.
        new_column (Optional[str]): The name of the new column for storing detection results.
        threshold (Optional[int]): The raw count threshold for detection or replacement.
        threshold_word_prop (Optional[float]): The proportion of words threshold for detection or replacement.
        threshold_char_prop (Optional[float]): The proportion of characters threshold for detection or replacement.
        replacement (Optional[str]): The text used to replace detected patterns when in 'replace' mode.
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[int] = None,
        threshold_word_prop: Optional[float] = None,
        threshold_char_prop: Optional[float] = None,
        replacement: Optional[str] = None,
    ):
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            threshold=threshold,
            threshold_word_prop=threshold_word_prop,
            threshold_char_prop=threshold_char_prop,
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
    A task for detecting or removing patterns in text data.

    This class extends `DetectOrRepairTask` to provide functionality for either
    detecting the presence of patterns or removing them from text data. It uses
    specified thresholds to determine whether a pattern should be flagged or removed.

    Args:

        column (str): The name of the column to process.
        mode (str): The operation mode, either 'detect' for flagging patterns or 'remove' to eliminate them.
        pattern (Optional[str]): The regular expression pattern used for detection or removal.
        new_column (Optional[str]): The name of the new column for storing detection results.
        threshold (Optional[int]): The raw count threshold for detection or removal.
        threshold_word_prop (Optional[float]): The proportion of words threshold for detection or removal.
        threshold_char_prop (Optional[float]): The proportion of characters threshold for detection or removal.
    """

    def __init__(
        self,
        column: str = "content",
        mode: str = "detect",
        pattern: Optional[str] = None,
        new_column: Optional[str] = None,
        threshold: Optional[int] = None,
        threshold_word_prop: Optional[float] = None,
        threshold_char_prop: Optional[float] = None,
    ):
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
            threshold=threshold,
            threshold_word_prop=threshold_word_prop,
            threshold_char_prop=threshold_char_prop,
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

    Args:

        mode (str): The mode of operation, either 'detect' or 'repair'.
        column (str): The name of the column to check for duplicates (default is "id").
        new_column (str): The name of the new column for detecting duplicates
            (default is "duplicate_review_id").
    """

    def __init__(
        self,
        mode: str = "detect",
        column: str = "id",
        new_column: str = "duplicate_review_id",
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

    Args:

        column (str): The name of the column to check for URLs.
        new_column (str): The name of the new column for URL detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' to flag URLs or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "url",
        replacement: str = "[URL]",
        mode: str = "detect",
    ) -> None:
        pattern = r"(https?:\/\/)?(www\.)?[\w\-_]+(\.[\w\-_]+)+([\/\w\-_\.]*)*"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR EMAIL ADDRESS TASK                                    #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairEmailAddressTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing email addresses in a DataFrame column.
    The class provides functionality to either flag the presence of email addresses in a
    new column or replace email addresses with a specified replacement string.

    Args:

        _column (str): The name of the column to check for email addresses (default is "content").
        _new_column (str): The name of the new column for email detection flags (default is "email").
        _replacement (str): The string to replace email addresses with in 'repair' mode (default is "[EMAIL]").
        _mode (str): The mode of operation, either 'detect' to flag email addresses or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_email",
        replacement: str = "[EMAIL]",
        mode: str = "detect",
    ) -> None:
        pattern = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                          DETECT OR REPAIR PHONE NUMBER TASK                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairPhoneNumberTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing phone numbers in a DataFrame column.
    The class provides functionality to either flag the presence of phone numbers in a
    new column or replace phone numbers with a specified replacement string.

    Args:

        _column (str): The name of the column to check for phone numbers (default is "content").
        _new_column (str): The name of the new column for phone number detection flags (default is "phone").
        _replacement (str): The string to replace phone numbers with in 'repair' mode (default is "[PHONE]").
        _mode (str): The mode of operation, either 'detect' to flag phone numbers or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_phone",
        replacement: str = "[PHONE]",
        mode: str = "detect",
    ) -> None:
        pattern = r"(\+?\d{1,3})?[\s.-]?\(?\d{2,4}\)?[\s.-]?\d{3,4}[\s.-]?\d{4}"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                       DETECT OR REPAIR EXCESSIVE SPECIAL CHARS TASK                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveSpecialCharsTask(DetectOrReplaceTask):
    """
    A task for detecting or repairing excessive special characters in text data.

    This class extends `DetectOrReplaceTask` and focuses on identifying or replacing
    excessive occurrences of specific special characters. The task uses a regular
    expression to detect characters like `#`, `<`, `>`, and `~` and applies a
    character proportion threshold to determine if the text should be flagged.

    Args:

        column (str): The name of the column to process.
        new_column (str): The name of the new column for storing detection results.
        threshold_char_prop (float): The proportion of characters threshold for detecting excessive special characters.
        mode (str): The operation mode, either 'detect' for flagging or 'repair' to replace the characters.
        replacement (str): The text used to replace detected special characters when in 'repair' mode.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_excess_special_chars",
        threshold_char_prop: float = 0.3,
        mode: str = "detect",
        replacement: str = " ",
    ) -> None:
        pattern = r"[#<>~]"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
            threshold_char_prop=threshold_char_prop,
            replacement=replacement,
        )


# ------------------------------------------------------------------------------------------------ #
#                         DETECT OR REPAIR NON ASCII CHARS TASK                                    #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonASCIICharsTask(DetectOrReplaceTask):
    """
    A task for detecting or repairing non-ASCII characters in text data.

    This class extends `DetectOrReplaceTask` to provide functionality for either
    detecting or repairing text that contains non-ASCII (Unicode) characters. In
    'detect' mode, it flags text with non-ASCII characters. In 'repair' mode, it
    attempts to normalize the text by removing non-ASCII characters and converting
    it to an ASCII-only format.

    Args:

        column (str): The name of the column to process.
        new_column (str): The name of the new column for storing detection results.
        mode (str): The operation mode, either 'detect' to flag non-ASCII characters or 'repair' to normalize the text.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "non_ascii_chars",
        mode: str = "detect",
    ) -> None:
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
#                          DETECT OR NON-ASCII TEXT TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonASCIITextTask(DetectOrRemoveTask):
    """
    A task for detecting or removing rows with excessive non-ASCII characters in text data.

    This class extends `DetectOrRemoveTask` to identify or remove rows where the proportion
    of non-ASCII (Unicode) characters exceeds a specified threshold. In 'detect' mode, it
    flags rows with excessive non-ASCII characters. In 'remove' mode, it filters out such rows
    from the DataFrame.

    Args:

        column (str): The name of the column to process.
        new_column (str): The name of the new column for storing detection results.
        threshold_char_prop (float): The proportion of non-ASCII characters required to flag or remove a row.
        mode (str): The operation mode, either 'detect' to flag excessive non-ASCII characters or 'remove' to filter out rows.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "excess_non_ascii_chars",
        threshold_char_prop: float = 0.2,
        mode: str = "detect",
    ) -> None:
        pattern = (
            r"[^\x00-\x7F]"  # Regex pattern to match non-ASCII (Unicode) characters
        )
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            threshold_char_prop=threshold_char_prop,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                        DETECT OR REPAIR CONTROL CHARS TASK                                       #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairControlCharsTask(DetectOrReplaceTask):
    """
    A PySpark task class for detecting or repairing control characters in a DataFrame column.
    The class provides functionality to either flag the presence of control characters or
    remove/replace them with a specified replacement string.

    Args:

        _column (str): The name of the column to check for control characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "ctrl_chars").
        _replacement (str): The string to replace control characters with in 'repair' mode (default is an empty string).
        _mode (str): The mode of operation, either 'detect' to flag control characters or 'repair' to replace them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_ctrl_chars",
        replacement: str = " ",
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

    Args:

        _column (str): The name of the column to check for HTML character entities (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "html_chars").
        _mode (str): The mode of operation, either 'detect' to flag HTML character entities or 'repair' to replace them.
        _replacement (str): The string to replace HTML character entities with in 'repair' mode (default is an empty string).
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_html_chars",
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

    Args:

        _column (str): The name of the column to check for excessive whitespace (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "excess_whitespace").
        _mode (str): The mode of operation, either 'detect' to flag excessive whitespace or 'repair' to replace it.
        _replacement (str): The string to replace excessive whitespace with in 'repair' mode (default is a single space).
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_excess_whitespace",
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

    Args:

        _column (str): The name of the column to check for accented characters (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "accented_chars").
        _mode (str): The mode of operation, either 'detect' to flag accented characters or 'repair' to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "has_accented_chars",
        mode: str = "detect",
    ) -> None:
        # Regex pattern for accented and diacritic characters
        pattern = r"[\u00C0-\u024F]"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            mode=mode,
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

    Args:

        _column (str): The name of the column to check for non-English text (default is "content").
        _new_column (str): The name of the new column for detection flags (default is "non_english").
        _mode (str): The mode of operation, either 'detect' to flag non-English text or 'remove' to delete such rows.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "non_english",
        mode: str = "detect",
    ) -> None:
        super().__init__(new_column=new_column)
        self._column = column

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

    Args:

        _column (str): The name of the column to check for character elongation (default is "content").
        _threshold (int): The minimum number of consecutive repeating characters to be considered elongation (default is 4).
        _new_column (str): The name of the new column for detection flags (default is "elongation").
        _max_elongation (int): The maximum allowed repetition of characters when repairing (default is 3).
        _mode (str): The mode of operation, either 'detect' to flag elongation or 'repair' to limit character repetition.
    """

    def __init__(
        self,
        column: str = "content",
        threshold: int = 4,
        new_column: str = "elongation",
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
#                           DETECT OR REPAIR REPEATED PATTERNS                                     #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedSequenceTask(DetectOrReplaceTask):
    """
    A task for detecting or repairing excessive sequence repetition in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated patterns within the specified text column. The task can
    be configured to either detect or repair these patterns.

    Args:

        column (str): The name of the column to analyze for repeated patterns.
        threshold_word_prop (float): The proportion of the words required to flag text
            as having excessive repetition.
        new_column (str): The name of the new column that will store the result
            of the detection (e.g., a boolean indicating excessive repetition).
        mode (str): The mode of operation, either "detect" for flagging repeated
            patterns or "repair" to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        length_of_sequence: int = 3,
        min_repetitions: int = 3,
        threshold: int = 3,
        new_column: str = "excess_sequence_repetition",
        mode: str = "detect",
    ) -> None:
        pattern = rf"(.{{{length_of_sequence},}})\1{{{min_repetitions - 1}}}"
        replacement = r"\1"
        super().__init__(
            pattern=pattern,
            column=column,
            threshold=threshold,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR REPEATED wWORDS                                       #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedWordsTask(DetectOrReplaceTask):
    """
    A task for detecting or repairing excessive repeated words in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated words within the specified text column. The task can
    be configured to either detect or repair these words

    Args:

        column (str): The name of the column to analyze for repeated words.
        threshold (int): The number of repeated words required to flag text
            as having excessive repetition.
        new_column (str): The name of the new column that will store the result
            of the detection (e.g., a boolean indicating excessive repetition).
        mode (str): The mode of operation, either "detect" for flagging repeated
            words or "repair" to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "excess_word_repetition",
        min_repetitions: int = 3,
        threshold: int = 1,
        mode: str = "detect",
    ) -> None:
        pattern = rf"(\b\w+\b)\s*(?:\1\s*){{{min_repetitions - 1},}}"
        replacement = r"\1"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            threshold=threshold,
            replacement=replacement,
            mode=mode,
        )


# ------------------------------------------------------------------------------------------------ #
#                           DETECT OR REPAIR REPEATED PHRASES                                      #
# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedPhrasesTask(DetectOrReplaceTask):
    """
    A task for detecting or repairing excessive repeated phrases in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated phrases within the specified text column. The task can
    be configured to either detect or repair these phrases

    Args:

        column (str): The name of the column to analyze for repeated phrases.
        threshold (int): The number of repeated phrases required to flag text
            as having excessive repetition.
        new_column (str): The name of the new column that will store the result
            of the detection (e.g., a boolean indicating excessive repetition).
        mode (str): The mode of operation, either "detect" for flagging repeated
            phrases or "repair" to remove them.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "excess_phrase_repetition",
        min_repetitions: int = 3,
        threshold: int = 3,
        mode: str = "detect",
    ) -> None:
        pattern = rf"(\b\w+\b(?: \b\w+\b)*)\s*(?:\1\s*){{{min_repetitions},}}"
        replacement = r"\1"
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            threshold=threshold,
            replacement=replacement,
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

    Args:

        _column (str): The name of the column to check for outliers (default is "review_length").
        _mode (str): The mode of operation, either 'detect' to flag outliers or 'remove' to delete such rows.
        _new_column (str): The name of the new column for outlier detection flags, dynamically generated based on the column name.
    """

    def __init__(
        self,
        column: str = "review_length",
        iqr_threshold: int = 3,
        new_column: str = "outlier_review_length",
        mode: str = "detect",
    ) -> None:
        pattern = None
        super().__init__(
            pattern=pattern,
            column=column,
            mode=mode,
            new_column=new_column,
        )
        self._iqr_threshold = iqr_threshold

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
        lower_bound = q1 - self._iqr_threshold * iqr
        upper_bound = q3 + self._iqr_threshold * iqr

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
class DetectOrRepairGibberishTask(DetectOrRemoveTask):
    def __init__(
        self,
        column: str = "an_perplexity",
        new_column: str = "gibberish",
        mode: str = "detect",
        ppl_filepath: str = "models/perplexity/perplexity_dev.csv",
    ) -> None:
        pattern = None
        super().__init__(
            pattern=pattern,
            column=column,
            mode=mode,
            new_column=new_column,
        )
        self._ppl_filepath = ppl_filepath

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
