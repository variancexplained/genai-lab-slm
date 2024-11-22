#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/strategy/text/local.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:34:06 am                                             #
# Modified   : Thursday November 21st 2024 05:57:25 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Text Cleaning Strategies for Local Devices"""
import unicodedata
from abc import abstractmethod
from typing import Literal, Type, Union

import pandas as pd

from discover.flow.task.clean.base.factory import StrategyFactory
from discover.flow.task.clean.base.strategy import DetectStrategy, RepairStrategy
from discover.flow.task.clean.strategy.text.pattern import Regex, RegexFactory


# ------------------------------------------------------------------------------------------------ #
class TextStrategyFactory(StrategyFactory):
    """Factory to retrieve strategies for anomaly detection and repair."""

    @property
    @abstractmethod
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex": RegexDetectStrategy,
            "regex_threshold": RegexThresholdDetectStrategy,
        }

    @property
    @abstractmethod
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex_replace": RegexReplaceStrategy,
            "regex_remove": RegexRemoveStrategy,
            "regex_threshold_remove": RegexThresholdRemoveStrategy,
            "accent": AccentRepairStrategy,
            "non_ascii": NonAsciiRepairStrategy,
        }


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
        self._new_column = new_column
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
    Removes rows from a DataFrame based on anomalies detected using a regex pattern.

    This strategy leverages a detection strategy to identify rows where the specified
    regex pattern matches. If the detection results column (`new_column`) does not exist
    in the DataFrame, the detection strategy is applied to generate it. Rows flagged as
    anomalies in the detection results column are removed.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to the value of `column`.
        regex_factory_cls (Type[RegexFactory], optional): The regex factory class to use.
            Defaults to `RegexFactory`.
        detect_strategy (Type[RegexDetectStrategy], optional): The detection strategy
            class to use for anomaly detection. Defaults to `RegexDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the detection strategy.

    Methods:
        repair(data: pd.DataFrame) -> pd.DataFrame:
            Removes rows from the DataFrame where anomalies are detected.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        regex_factory_cls: Type[RegexFactory] = RegexFactory,
        detect_strategy: Type[RegexDetectStrategy] = RegexDetectStrategy,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column
        self._regex_factory = regex_factory_cls()
        self._detect_strategy = detect_strategy
        self._kwargs = kwargs

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows from the DataFrame where anomalies are detected.

        If the detection results column (`new_column`) is not present in the DataFrame,
        the associated detection strategy is applied to generate it. Rows flagged as
        anomalies in the detection results column are then filtered out.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: A new DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the column specified for detection does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data.
        """

        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data=data)

        # Filter out rows where anomalies are detected
        return data.loc[~data[self._new_column]]


# ------------------------------------------------------------------------------------------------ #
class RegexThresholdDetectStrategy(RegexDetectStrategy):
    """
    Detects anomalies in a text column based on a regex pattern and a dynamic threshold.

    This strategy evaluates matches of a regex pattern against a specified threshold.
    The threshold can be a fixed count of matches or a proportion relative to a unit
    (e.g., words or characters).

    Args:
        pattern (str): The regex pattern used for detection.
        column (str): The name of the column to evaluate.
        new_column (str): The name of the column to store detection results.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".
        regex_factory_cls (Type[RegexFactory], optional): The regex factory class to use. Defaults
            to `RegexFactory`.
        **kwargs: Additional keyword arguments passed to the regex factory.

    Methods:
        detect(data: pd.DataFrame) -> pd.DataFrame:
            Detects anomalies in the specified column based on the regex pattern and threshold.
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
class RegexThresholdRemoveStrategy(RegexRemoveStrategy):
    """
    Removes rows from a DataFrame based on anomalies detected using a regex pattern and a dynamic threshold.

    This strategy combines regex pattern matching with a threshold-based detection strategy. Anomalies
    are identified based on either the count of matches or their proportion relative to a unit (e.g.,
    words or characters). Rows flagged as anomalies are removed.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str): The name of the column to store detection results.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for a
            fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".
        regex_factory_cls (Type[RegexFactory], optional): The regex factory class to use. Defaults to `RegexFactory`.
        detect_strategy (Type[RegexThresholdDetectStrategy], optional): The detection strategy
            class to use for threshold-based anomaly detection. Defaults to `RegexThresholdDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the detection strategy.

    Methods:
        repair(data: pd.DataFrame) -> pd.DataFrame:
            Removes rows from the DataFrame where anomalies are detected.
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
        detect_strategy: Type[
            RegexThresholdDetectStrategy
        ] = RegexThresholdDetectStrategy,
        **kwargs,
    ) -> None:
        self._pattern = pattern
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._threshold_type = threshold_type
        self._unit = unit
        self._regex_factory = regex_factory_cls()
        self._detect_strategy = detect_strategy
        self._kwargs = kwargs

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes rows from the DataFrame where anomalies are detected.

        If the detection results column (`new_column`) is not present in the DataFrame,
        the associated threshold-based detection strategy is applied to generate it. Rows
        flagged as anomalies in the detection results column are then filtered out.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: A new DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the column specified for detection does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data or invalid arguments
                are provided to the detection strategy.
        """

        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                threshold=self._threshold,
                threshold_type=self._threshold_type,
                unit=self._unit,
                **self._kwargs,
            )
            data = strategy.detect(data=data)

        # Filter out rows where anomalies are detected
        return data.loc[~data[self._new_column]]


# ------------------------------------------------------------------------------------------------ #
class CustomRegexRepairStrategy(RegexReplaceStrategy):
    """
    Abstract base class for custom text repair strategies using regex-based detection.

    This strategy detects anomalies in a specified column based on a regex pattern
    and applies a custom text repair operation to rows where anomalies are detected.
    Subclasses must implement the `repair_text` method to define the specific repair logic.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        replacement (str, optional): The replacement value for the regex replace operation.
            Defaults to None.
        detect_strategy (Type[RegexDetectStrategy], optional): The detection strategy
            class to use for identifying anomalies. Defaults to `RegexDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the parent class or the detection strategy.

    Methods:
        repair(data: pd.DataFrame) -> pd.DataFrame:
            Detects anomalies using the specified detection strategy and applies the
            custom repair logic to rows where anomalies are flagged.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        replacement: str = None,
        detect_strategy: Type[RegexDetectStrategy] = RegexDetectStrategy,
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=pattern, column=column, new_column=new_column, **kwargs
        )
        self._detect_strategy = detect_strategy

    def repair(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects anomalies and applies custom repair logic.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the specified detection strategy is used to generate it. The `repair_text` method
        is then applied to rows where anomalies are flagged.

        Args:
            data (pd.DataFrame): The input DataFrame to process.

        Returns:
            pd.DataFrame: A new DataFrame with repaired text in the specified column.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data.
        """
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data=data)
        data.loc[data[self._new_column] is True, self._column] = data.loc[
            data[self._new_column] is True, self._column
        ].parallel_apply(self.repair_text)

    @abstractmethod
    @staticmethod
    def repair_text(text):
        """
        Abstract method to define custom text repair logic.

        Subclasses must implement this method to specify how text in rows flagged
        as anomalies should be repaired.

        Args:
            text (str): The input text to repair.

        Returns:
            str: The repaired text.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class AccentRepairStrategy(CustomRegexRepairStrategy):
    """
    A strategy for repairing text by removing accents and diacritics.

    This strategy detects anomalies based on a regex pattern and repairs the text
    by removing accents and diacritical marks from characters. It extends the
    `CustomRepairStrategy` and defines the specific repair logic in the `repair_text` method.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        replacement (str, optional): A placeholder argument inherited from `CustomRepairStrategy`.
            Not used in this class but included for compatibility.
        **kwargs: Additional keyword arguments passed to the parent class.

    Methods:
        repair_text(text: str) -> str:
            Removes accents and diacritics from a given text string.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        replacement: str = None,
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=pattern, column=column, new_column=new_column, **kwargs
        )

    @staticmethod
    def repair_text(text):
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
class NonAsciiRepairStrategy(CustomRegexRepairStrategy):
    """
    A strategy for repairing text by removing non-ASCII characters.

    This strategy detects anomalies based on a regex pattern and repairs text
    by converting it to an ASCII-compatible format. It removes any non-ASCII
    characters while retaining the closest ASCII representation when possible.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        replacement (str, optional): A placeholder argument inherited from `CustomRepairStrategy`.
            Not used in this class but included for compatibility.
        **kwargs: Additional keyword arguments passed to the parent class.

    Methods:
        repair_text(text: str) -> str:
            Converts the text to ASCII by removing or replacing non-ASCII characters.
    """

    def __init__(
        self,
        pattern: str,
        column: str,
        new_column: str = None,
        replacement: str = None,
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=pattern, column=column, new_column=new_column, **kwargs
        )

    @staticmethod
    def repair_text(text):
        """
        Converts the text to ASCII by removing or replacing non-ASCII characters.

        This method normalizes the input text to the NFKD form, which separates
        base characters and diacritics. Non-ASCII characters are then removed
        while preserving the closest ASCII-compatible representation.

        Args:
            text (str): The input string with potential non-ASCII characters.

        Returns:
            str: The text converted to ASCII, with non-ASCII characters removed.
        """
        normalized_text = unicodedata.normalize("NFKD", text)
        ascii_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return ascii_text
