#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/incubator/flow/data_prep/tqa/task.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Monday November 4th 2024 11:12:36 pm                                                #
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
#                                      TQA TASK                                                    #
# ------------------------------------------------------------------------------------------------ #
class TQATask(Task):
    """
    A base class for data quality assurance (TQA) tasks.

    This class provides the foundation for tasks that assess data quality on a specified column and
    flag anomalies in a corresponding TQA column. Subclasses must implement the core logic of the
    task in the `run` method.

    Attributes:
    -----------
    dqa_column : str
        The name of the column where the results of the TQA check (anomalies) will be stored.
    column : str
        The name of the column that the TQA task will inspect. Optional. If not provided, all
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
        Initializes the TQATask with the column to inspect and the column to store the results.

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
        Summarizes the anomalies detected by the TQA task and prints the results.

        This method calculates the total number of anomalies detected in the `dqa_column`
        and provides a percentage of how many records contain anomalies.

        Parameters:
        -----------
        data : pd.DataFrame
            The DataFrame containing the data and the corresponding TQA results.

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
class EntropyTask(TQATask):
    """
    A data quality assurance (TQA) task that checks for high entropy in a specified text column.

    This task calculates the Shannon entropy of text data in a specified column and flags entries
    where the entropy exceeds a predefined threshold. High entropy often indicates more complex
    or less predictable text, which might be used as a signal for certain quality checks.

    Attributes:
    -----------
    _column : str
        The name of the column containing the text data to inspect.
    _dqa_column : str
        The name of the column where the TQA results (entropy flags) will be stored.
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
        Initializes the EntropyTask with a target column, a TQA column for flags, and a threshold for entropy.

        Args:
        -----
        column : str, optional
            The name of the column containing the text data to inspect. Default is "content".
        dqa_column : str, optional
            The name of the column where the TQA results (entropy flags) will be stored. Default is "entropy".
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
        and flags those that exceed the defined threshold by setting the value in the TQA column.

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
#                            DETECT SPECIAL CHARACTERS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectSpecialCharacterTask(TQATask):
    """
    A data quality assurance (TQA) task that flags rows with excessive special characters in the text.

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
        Executes the task to detect texts with excessive special characters and flags them in the TQA column.
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
        Initializes the DetectSpecialCharacterTask with the specified column, TQA column, and threshold.

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
class DetectProfanityTask(TQATask):
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


class DetectSpellingErrorsTask(TQATask):
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


# ------------------------------------------------------------------------------------------------ #
#                                    DETECT LONG REVIEWS                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectLongReviewsTask(TQATask):
    """Detects zero-length reviews.

    Args:
        column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        iqr_factor: int = 3,
        column: str = "eda_review_length",
        dqa_column: str = "b_long_review",
    ):
        super().__init__()
        self._iqr_factor = iqr_factor
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
        q1, q3 = self._df[self._column].quantile([0.25, 75])
        iqr = q3 - q1

        result = data[self._column] > self._iqr_factor * iqr
        result = result.rename(self._dqa_column)

        self.summarize(data=result)

        return result
