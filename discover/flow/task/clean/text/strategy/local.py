#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/text/strategy/local.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:34:06 am                                             #
# Modified   : Thursday November 21st 2024 04:09:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Strategy Base Class Module"""
from typing import Literal, Type, Union

import pandas as pd

from discover.flow.task.clean.base.strategy import DetectStrategy, RepairStrategy
from discover.flow.task.clean.text.pattern import Regex, RegexFactory


# ------------------------------------------------------------------------------------------------ #
class RegexDetectStrategy(DetectStrategy):
    """
    Detects anomalies in data based on a static regex pattern.

    Args:
        pattern (str): The name of the regex pattern to use.
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection results.
        regex_factory_cls (Type[RegexFactory]): The factory class for retrieving regex patterns.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str,
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column
        self._regex_factory = regex_factory_cls()
        self._kwargs = kwargs

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies in the specified column using a regex pattern.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing detection results.

        Raises:
            KeyError: If the specified column does not exist.
            ValueError: If the regex pattern is invalid or cannot be applied.
        """
        try:
            # Retrieve the regex pattern from the factory
            regex_info: Regex = self._regex_factory.get_regex(
                pattern=self._pattern, **self._kwargs
            )

            # Apply the regex pattern to detect anomalies
            data[self._new_column] = data[self._column].str.contains(
                regex_info.pattern, regex=True, na=False
            )
        except KeyError:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")
        except Exception as e:
            raise ValueError(f"Failed to apply regex pattern: {self._pattern}\n{e}")

        return data


# ------------------------------------------------------------------------------------------------ #
class RegexReplaceStrategy(RepairStrategy):
    """
    Repairs anomalies in data based on a static regex pattern by replacing matched values.

    Args:
        pattern (str): The name of the regex pattern to use.
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store repaired results.
        replacement (str, optional): Custom replacement string. Defaults to the factory-defined replacement.
        regex_factory_cls (Type[RegexFactory]): The factory class for retrieving regex patterns.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        replacement: str = None,
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column or column
        self._replacement = replacement
        self._regex_factory = regex_factory_cls()

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Repairs anomalies in the specified column by replacing matched values.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: The DataFrame with a new or modified column containing repaired results.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the replacement value is invalid.
        """
        # Retrieve the regex pattern and replacement from the factory
        regex_info: Regex = self._regex_factory.get_regex(pattern=self._pattern)
        replacement = self._replacement or regex_info.replacement

        if not isinstance(replacement, str):
            raise ValueError(f"Invalid replacement value: {replacement}")

        try:
            # Apply regex replacement
            data[self._new_column] = data[self._column].str.replace(
                regex_info.pattern, replacement, regex=True
            )
        except KeyError:
            raise KeyError(
                f"Column '{self._column}' does not exist in the DataFrame. "
                f"Available columns: {data.columns.tolist()}"
            )

        return data


# ------------------------------------------------------------------------------------------------ #
class RegexRemoveStrategy(RepairStrategy):
    """
    Removes rows from a DataFrame based on a regex pattern.

    If the detection column (`new_column`) doesn't exist, the strategy uses its
    own private `_detect` method to identify rows to remove.

    Args:
        pattern (str): The name of the regex pattern to use.
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection results.
        regex_factory_cls (Type[RegexFactory]): The factory class for retrieving regex patterns.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column or column
        self._regex_factory = regex_factory_cls()
        self._kwargs = kwargs

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows from the DataFrame where the regex pattern matches.

        If the detection column doesn't exist, it runs the `_detect` method to create it.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: The DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
        """
        if self._new_column not in data.columns:
            data = self._detect(data=data)

        # Filter out rows where anomalies are detected
        return data.loc[~data[self._new_column]]

    def _detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects anomalies in the specified column using a regex pattern.

        Args:
            data (pd.DataFrame): The input DataFrame to analyze.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing detection results.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the regex pattern is invalid or cannot be applied.
        """
        # Retrieve the regex pattern from the factory
        regex_info = self._regex_factory.get_regex(
            pattern=self._pattern, **self._kwargs
        )

        try:
            # Apply the regex pattern to detect anomalies
            data[self._new_column] = data[self._column].str.contains(
                regex_info.pattern, regex=True, na=False
            )
        except KeyError:
            raise KeyError(
                f"Column '{self._column}' does not exist in the DataFrame. "
                f"Available columns: {data.columns.tolist()}"
            )
        except Exception as e:
            raise ValueError(f"Failed to apply regex pattern: {self._pattern}\n{e}")

        return data


# ------------------------------------------------------------------------------------------------ #
class DynamicThresholdDetectStrategy(RegexDetectStrategy):
    """
    Detects anomalies in data based on a dynamic threshold using regex patterns.

    Args:
        pattern (str): The name of the regex pattern to use.
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection results.
        threshold (Union[float, int]): The threshold value to trigger an anomaly.
        threshold_type (Literal["count", "proportion"]): Type of threshold ("count" or "proportion").
        unit (Literal["word", "character"], optional): The unit of measurement for proportions. Defaults to "word".
        regex_factory_cls (Type[RegexFactory], optional): Factory class for retrieving regex patterns.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str,
        threshold: Union[float, int],
        threshold_type: Literal["count", "proportion"],
        unit: Literal["word", "character"] = "word",
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._threshold_type = threshold_type
        self._unit = unit
        self._regex_factory = regex_factory_cls()
        self._kwargs = kwargs

        # Validate input arguments
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        """
        Validates the initialization parameters.
        """
        if self._threshold_type == "proportion" and self._unit not in {
            "word",
            "character",
        }:
            raise ValueError(
                "Unit must be 'word' or 'character' when threshold_type is 'proportion'."
            )
        if self._threshold_type not in {"count", "proportion"}:
            raise ValueError("Threshold type must be 'count' or 'proportion'.")

    def detect(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects anomalies in the specified column based on a dynamic threshold.

        Args:
            data (pd.DataFrame): The input DataFrame to analyze.

        Returns:
            pd.DataFrame: The DataFrame with a new column containing detection results.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the regex pattern is invalid or cannot be applied.
        """
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Retrieve the regex pattern from the factory
        regex_info = self._regex_factory.get_regex(
            pattern=self._pattern, **self._kwargs
        )

        # Apply regex to detect matches
        try:
            matches = data[self._column].str.findall(regex_info.pattern, na=False)
        except Exception as e:
            raise ValueError(f"Failed to apply regex pattern: {self._pattern}\n{e}")

        # Calculate thresholds
        if self._threshold_type == "count":
            data[self._new_column] = matches.apply(len) > self._threshold
        elif self._threshold_type == "proportion":
            if self._unit == "word":
                proportions = (
                    matches.str.len() / data[self._column].str.split().str.len()
                )
            elif self._unit == "character":
                proportions = matches.str.len() / data[self._column].str.len()
            else:
                raise ValueError(f"Unsupported unit: {self._unit}")
            data[self._new_column] = proportions > self._threshold

        return data


# ------------------------------------------------------------------------------------------------ #
class DynamicReplaceStrategy(RepairStrategy):

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str,
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column
        self._regex_factory = regex_factory_cls()
        self._kwargs = kwargs

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Retrieve the regex pattern from the factory
        regex_info = self._regex_factory.get_regex(
            pattern=self._pattern, **self._kwargs
        )

        # Apply regex to detect matches
        try:
            matches = data[self._column].str.findall(regex_info.pattern, na=False)
        except Exception as e:
            raise ValueError(f"Failed to apply regex pattern: {self._pattern}\n{e}")

        # Calculate thresholds
        if self._threshold_type == "count":
            data[self._new_column] = matches.apply(len) > self._threshold
        elif self._threshold_type == "proportion":
            if self._unit == "word":
                proportions = (
                    matches.str.len() / data[self._column].str.split().str.len()
                )
            elif self._unit == "character":
                proportions = matches.str.len() / data[self._column].str.len()
            else:
                raise ValueError(f"Unsupported unit: {self._unit}")
            data[self._new_column] = proportions > self._threshold

        return data
