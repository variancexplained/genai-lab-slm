#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/analysis/dqa.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 2nd 2024 08:36:42 pm                                                    #
# Modified   : Thursday July 4th 2024 07:39:15 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
import os
import re
import warnings
from typing import List, Optional, Type, Union

import emoji
import fasttext
import numpy as np
import pandas as pd
from pandarallel import pandarallel
from profanity_check import predict

from appinsight.analysis.base import Analysis
from appinsight.shared.instrumentation.decorator import task_profiler
from appinsight.shared.logging.logging import log_exceptions
from appinsight.shared.persist.file.io import IOService
from appinsight.shared.persist.object.cache import cachenow
from appinsight.utils.format import format_numerics
from appinsight.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=8, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")

# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None


# ------------------------------------------------------------------------------------------------ #
class DataQualityAnalysis(Analysis):
    """Data quality analysis class.

    Attributes:
        data (pd.DataFrame): The input data for quality analysis.

    Args:
        text_column (str): Name of column containing review text.
        duplicate_column (str): Name of column upon which to evaluate duplication
        excessiev_special_chars_threshold (float): The proporiton of special chars in a review, above
            which is considered excessive.
    """

    def __init__(
        self,
        text_column: str = "content",
        duplicate_column: str = "id",
        excessive_special_chars_threshold: float = 0.3,
        repo: Type[ReviewRepo] = ReviewRepo,
    ) -> None:
        super().__init__()
        self._data = None
        self._text_column = text_column
        self._duplicate_column = duplicate_column
        self._excessive_special_chars_threshold = excessive_special_chars_threshold
        self._repo = repo()
        self.config = IOService.read(filepath="config/clean.csv")

    @property
    def data(self) -> pd.DataFrame:
        return self._data

    @cachenow()
    def _analyze_duplication(
        self,
        column_names: Optional[Union[List[str], str]] = None,
    ) -> int:
        """Analyzes duplication in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.
            column_names (Optional[Union[List[str], str]], optional): Column names to check for
            duplicates. Defaults to None.

        Returns:
            int: The number of duplicate rows.
        """
        if column_names:
            print(f"Analyzing Duplication by {column_names}")
        else:
            print("Analyzing Duplication")
        return np.sum(self._data.duplicated(subset=column_names, keep="first"))

    @cachenow()
    def _analyze_null_values(self) -> int:
        """Analyzes null values in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows with null values.
        """
        print("Analyzing Null Values")
        return np.sum(self._data.isnull().values.any(axis=1))

    @cachenow()
    def _analyze_non_english(self, text_column: str = None) -> int:
        """Analyzes non-English reviews in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of non-English reviews.

        Raises:
            RuntimeError: If the FastText model cannot be loaded.
        """
        print("Analyzing Non-English Data")
        text_column = text_column or self._text_column
        try:
            model_filepath = os.getenv("FASTTEXT_MODEL")
            model = fasttext.load_model(model_filepath)
        except Exception as e:
            self.logger.error("Error loading FastText model.")
            raise RuntimeError("FastText model loading failed.") from e

        def is_non_english(text):
            predictions = model.predict(text)
            return predictions[0][0] != "__label__en"

        mask = self._data[text_column].apply(is_non_english)
        return mask.sum()

    @cachenow()
    def _analyze_emoji(self) -> int:
        """Analyzes emojis in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows containing emojis.
        """
        print("Analyzing Emojis")

        def contains_emoji(text):
            return any(char in emoji.EMOJI_DATA for char in text)

        mask = self._data[self._text_column].parallel_apply(contains_emoji)
        return mask.sum()

    @cachenow()
    def _analyze_special_characters(self) -> int:
        """Analyzes excessive special characters in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows with excessive special characters.
        """
        print("Analyzing Excessive Special Characters")

        def contains_excessive_special_chars(text):
            special_chars = re.findall(r"[^\w\s]", text)
            return (
                len(special_chars) / len(text) > self._excessive_special_chars_threshold
                if len(text) > 0
                else False
            )

        mask = self._data[self._text_column].parallel_apply(
            contains_excessive_special_chars
        )
        return mask.sum()

    @cachenow()
    def _analyze_invalid_dates(self) -> int:
        """Analyzes invalid dates in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows with invalid dates.
        """
        print("Analyzing Invalid Dates")

        def date_invalid(date: np.datetime64):
            date = np.datetime64(date)
            return date < np.datetime64("2007") or date > np.datetime64("2024")

        mask = self._data["date"].parallel_apply(date_invalid)
        return mask.sum()

    @cachenow()
    def _analyze_invalid_ratings(self) -> int:
        """Analyzes invalid ratings in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows with invalid ratings.
        """
        print("Analyzing Invalid Ratings")

        def rating_invalid(rating: int):
            return rating < 0 or rating > 5

        mask = self._data["rating"].parallel_apply(rating_invalid)
        return mask.sum()

    @cachenow()
    def _analyze_profanity(self) -> int:
        """Analyzes profanity in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows containing profanity.
        """
        print("Analyzing Profanity")

        def contains_profanity(text):
            if pd.isnull(text):
                return False
            return predict([text])[0] == 1

        # Apply the function to each row in parallel and get the boolean mask
        mask = self._data[self._text_column].parallel_apply(contains_profanity)

        # Count the number of True values in the boolean mask
        return mask.sum()

    @cachenow()
    def _analyze_email(self) -> int:
        """Analyzes email addresses in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows containing email addresses.
        """
        print("Analyzing Emails in Reviews")

        def contains_email(text):
            email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
            return bool(re.search(email_pattern, text))

        mask = self._data[self._text_column].parallel_apply(contains_email)
        return mask.sum()

    @cachenow()
    def _analyze_urls(self) -> int:
        """Analyzes URLs in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows containing URLs.
        """
        print("Analyzing URLs in Reviews")

        def contains_urls(text):
            url_pattern = (
                r"\b(?:https?://|www\.)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?\b"
            )
            return bool(re.search(url_pattern, text))

        mask = self._data[self._text_column].parallel_apply(contains_urls)
        return mask.sum()

    @cachenow()
    def _analyze_phone_number(self) -> int:
        """Analyzes phone numbers in the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            int: The number of rows containing phone numbers.
        """
        print("Analyzing Phone Numbers in Reviews")

        def contains_phone_number(text):
            phone_number_pattern = r"\b(?:\d{3}[-.]?|\(\d{3}\) )?\d{3}[-.]?\d{4}\b"
            return bool(re.search(phone_number_pattern, text))

        mask = self._data[self._text_column].parallel_apply(contains_phone_number)
        return mask.sum()

    @cachenow()
    def _analyze_outliers(self, column: str) -> int:
        """Analyzes outliers in a specified column of the dataset.

        Args:
            data (pd.DataFrame): The data to analyze.
            column (str): The column to check for outliers.

        Returns:
            int: The number of outliers.
        """
        print(f"Analyzing Outliers in {column}")
        # Compute the inter-quartile range
        q1 = self._data[column].quantile(0.25)
        q3 = self._data[column].quantile(0.75)
        iqr = q3 - q1

        # Compute lower and upper thresholds
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)

        # Flag observations
        mask = np.where(
            (self._data[column] < lower) | (self._data[column] > upper), True, False
        )
        return mask.sum()

    @cachenow()
    def _add_impact(self, result: pd.DataFrame) -> pd.DataFrame:
        """Adds impact scores to the analysis results.

        Args:
            data (pd.DataFrame): The data to annotate with impact scores.

        Returns:
            pd.DataFrame: The annotated data.
        """
        result = result.merge(self.config, how="left", on="Characteristic")
        result = result[["Characteristic", "Impact", "Count", "Percent"]].sort_values(
            by="Impact"
        )
        return result

    @cachenow()
    def _compute_metrics(self) -> pd.DataFrame:
        """Computes various data quality metrics.

        Args:
            data (pd.DataFrame): The data to analyze.

        Returns:
            pd.DataFrame: The computed metrics.
        """
        metrics = {}
        metrics["Duplicate Values"] = self._analyze_duplication()
        metrics["Duplicate IDs"] = self._analyze_duplication(
            column_names=self._duplicate_column
        )
        metrics["Null Values"] = self._analyze_null_values()
        metrics["Non-English Review"] = self._analyze_non_english()
        metrics["Non-English App Name"] = self._analyze_non_english(
            text_column="app_name"
        )
        metrics["Emojis"] = self._analyze_emoji()
        metrics["Excessive Special Characters"] = self._analyze_special_characters()
        metrics["Invalid Dates"] = self._analyze_invalid_dates()
        metrics["Invalid Ratings"] = self._analyze_invalid_ratings()
        metrics["Profanity"] = self._analyze_profanity()
        metrics["Contains Email Address(es)"] = self._analyze_email()
        metrics["Contains URL(s)"] = self._analyze_urls()
        metrics["Contains Phone Number(s)"] = self._analyze_phone_number()
        metrics["Contains Vote Count Outliers"] = self._analyze_outliers(
            column="vote_count"
        )
        metrics["Contains Vote Sum Outliers"] = self._analyze_outliers(
            column="vote_sum"
        )

        df = pd.DataFrame(data=metrics, index=[0]).T.reset_index()
        df.columns = ["Characteristic", "Count"]
        df = format_numerics(df)
        df["Percent"] = round(df["Count"] / len(self._data) * 100, 2)
        return df

    @task_profiler()
    @log_exceptions()
    @cachenow()
    def run_analysis(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the data quality analysis task.

        Args:
            data (pd.DataFrame): Data to be analyzed

        Returns:
            pd.DataFrame: The results of the analysis.
        """
        self._data = data
        result = self._compute_metrics()
        result = self._add_impact(result=result)
        return result
