#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/data_prep/clean.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 04:36:23 pm                                                #
# Modified   : Monday October 21st 2024 12:06:20 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Module"""

import re
from abc import abstractmethod

import pandas as pd

from discover.flow.base.task import Task
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
#                                   DATA CLEANER                                                   #
# ------------------------------------------------------------------------------------------------ #
class DataCleaner(Task):
    """
    A class for cleaning a review dataset by removing rows that have anomalies based on a binary indicator.

    This class filters out rows in the dataset where the specified `dqa_column` has a value of `True`, which indicates
    the presence of a data quality anomaly. By default, the column used for this check is `"dqa_entropy"`, but it can
    be customized by providing a different column name during initialization.

    Attributes:
    ----------
    dqa_column : str
        The name of the column containing the data quality anomaly indicators (default is "dqa_entropy").

    Methods:
    -------
    run(data: pd.DataFrame) -> pd.DataFrame
        Filters the input DataFrame, removing rows where the `dqa_column` value is `True`.
    """

    def __init__(
        self,
        dqa_column: str = "dqa_entropy",
    ):
        super().__init__()
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.loc[data[self._dqa_column] is not True]


# ------------------------------------------------------------------------------------------------ #
#                             SENSITIVE INFORMATION MASK BASE CLASS                                #
# ------------------------------------------------------------------------------------------------ #
class SensitiveInformationMaskTask(Task):
    """
    A base class for tasks that mask sensitive information in text columns of a DataFrame.

    This class is designed to be subclassed for specific types of sensitive information,
    such as phone numbers, email addresses, and URLs. The class applies a regex pattern
    to mask sensitive information in a specified column of the DataFrame.

    Attributes:
        _text_column (str): The column name in the DataFrame that contains the text to be processed.
        _mask (str): The string used to replace the sensitive information in the text.
    """

    def __init__(
        self,
        mask: str,
        text_column: str = "content",
    ):
        """
        Initializes the SensitiveInformationMaskTask class.

        Args:
            mask (str): The mask that will replace sensitive information.
            text_column (str): The name of the column containing the text to be processed. Default is "content".
        """
        super().__init__()
        self._text_column = text_column
        self._mask = mask

    @property
    @abstractmethod
    def pattern(self) -> str:
        """
        Abstract property that returns the regex pattern to identify sensitive information.

        Returns:
            str: The regex pattern used to find sensitive information in the text.
        """
        pass

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Runs the task to mask sensitive information in the specified column of the DataFrame.

        Args:
            data (pd.DataFrame): The input DataFrame containing text data.

        Returns:
            pd.DataFrame: A DataFrame with sensitive information masked in the specified text column.
        """
        data[self._text_column] = data[self._text_column].parallel_apply(
            self._mask_info
        )
        return data

    def _mask_info(self, text) -> str:
        """
        Masks sensitive information in the given text.

        Args:
            text (str): The text to be processed.

        Returns:
            str: The text with sensitive information masked.
        """
        return re.sub(self.pattern, self._mask, text)


class PhoneNumberMaskTask(SensitiveInformationMaskTask):
    """
    A task to mask phone numbers in the specified text column of a DataFrame.

    Inherits from SensitiveInformationMaskTask and defines a regex pattern to find
    and replace phone numbers with a specified mask.
    """

    def __init__(
        self,
        mask: str = "[PHONE_MASK]",
        text_column: str = "content",
    ):
        """
        Initializes the PhoneNumberMaskTask class.

        Args:
            mask (str): The mask that will replace phone numbers. Default is "[PHONE_MASK]".
            text_column (str): The name of the column containing the text to be processed. Default is "content".
        """
        super().__init__(mask=mask, text_column=text_column)

    @property
    def pattern(self) -> str:
        """
        Returns the regex pattern for identifying phone numbers.

        Returns:
            str: The regex pattern used to find phone numbers in the text.
        """
        return r"\+?\d[\d -]{8,}\d"


class EmailMaskTask(SensitiveInformationMaskTask):
    """
    A task to mask email addresses in the specified text column of a DataFrame.

    Inherits from SensitiveInformationMaskTask and defines a regex pattern to find
    and replace email addresses with a specified mask.
    """

    def __init__(
        self,
        mask: str = "[EMAIL_MASK]",
        text_column: str = "content",
    ):
        """
        Initializes the EmailMaskTask class.

        Args:
            mask (str): The mask that will replace email addresses. Default is "[EMAIL_MASK]".
            text_column (str): The name of the column containing the text to be processed. Default is "content".
        """
        super().__init__(mask=mask, text_column=text_column)

    @property
    def pattern(self) -> str:
        """
        Returns the regex pattern for identifying email addresses.

        Returns:
            str: The regex pattern used to find email addresses in the text.
        """
        return r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"


class URLMaskTask(SensitiveInformationMaskTask):
    """
    A task to mask URLs in the specified text column of a DataFrame.

    Inherits from SensitiveInformationMaskTask and defines a regex pattern to find
    and replace URLs with a specified mask.
    """

    def __init__(
        self,
        mask: str = "[URL_MASK]",
        text_column: str = "content",
    ):
        """
        Initializes the URLMaskTask class.

        Args:
            mask (str): The mask that will replace URLs. Default is "[URL_MASK]".
            text_column (str): The name of the column containing the text to be processed. Default is "content".
        """
        super().__init__(mask=mask, text_column=text_column)

    @property
    def pattern(self) -> str:
        """
        Returns the regex pattern for identifying URLs.

        Returns:
            str: The regex pattern used to find URLs in the text.
        """
        return r"(https?://\S+|www\.\S+)"
