#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/strategy/text/distributed.py                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 03:13:48 am                                             #
# Modified   : Friday November 22nd 2024 02:12:24 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import unicodedata
from typing import Literal, Type, Union

import fasttext
from lingua import Language, LanguageDetectorBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, DoubleType, StringType

from discover.flow.task.clean.base.factory import StrategyFactory
from discover.flow.task.clean.base.strategy import DetectStrategy, RepairStrategy
from discover.flow.task.clean.strategy.text.pattern import RegexFactory

# ------------------------------------------------------------------------------------------------ #
languages = [Language.ENGLISH, Language.SPANISH]
detector = LanguageDetectorBuilder.from_languages(*languages).build()
# ------------------------------------------------------------------------------------------------ #
fasttext.FastText.eprint = lambda x: None  # Suppress FastText warnings
fasttext_model = fasttext.load_model("models/language_detection/lid.176.bin")


# ------------------------------------------------------------------------------------------------ #
class TextStrategyFactory(StrategyFactory):
    """Factory to retrieve strategies for anomaly detection and repair."""

    @property
    def detect_strategies(self) -> dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex": RegexDetectStrategy,
            "regex_threshold": RegexThresholdDetectStrategy,
            "non_english": NonEnglishDetectStrategy,
        }

    @property
    def repair_strategies(self) -> dict[str, Type[RepairStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex_replace": RegexReplaceStrategy,
            "regex_remove": RegexRemoveStrategy,
            "regex_threshold_remove": RegexThresholdRemoveStrategy,
            "accent": AccentRepairStrategy,
            "non_ascii": NonAsciiRepairStrategy,
            "non_english": NonEnglishRemovalStrategy,
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

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detect anomalies in the specified column using a regex pattern.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: The DataFrame with a new column containing detection results.

        Raises:
            KeyError: If the specified column does not exist.
            ValueError: If the regex pattern is invalid or cannot be applied.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        try:
            # Retrieve the regex pattern from the factory
            regex_info = self._regex_factory.get_regex(
                pattern=self._pattern, **self._kwargs
            )

            # Apply the regex pattern to detect anomalies
            data = data.withColumn(
                self._new_column, F.col(self._column).rlike(regex_info.pattern)
            )
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
        self._new_column = (
            new_column  # Use the same column if no new column is specified
        )
        self._replacement = replacement
        self._regex_factory = regex_factory_cls()

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Repairs anomalies in the specified column by replacing matched values.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: The DataFrame with a new or modified column containing repaired results.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the replacement value is invalid.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Retrieve the regex pattern and replacement from the factory
        regex_info = self._regex_factory.get_regex(pattern=self._pattern)
        replacement = self._replacement or regex_info.replacement

        if not isinstance(replacement, str):
            raise ValueError(f"Invalid replacement value: {replacement}")

        try:
            # Apply regex replacement
            data = data.withColumn(
                self._new_column,
                F.regexp_replace(F.col(self._column), regex_info.pattern, replacement),
            )
        except Exception as e:
            raise ValueError(f"Failed to apply regex replacement: {e}")

        return data


# ------------------------------------------------------------------------------------------------ #
class RegexRemoveStrategy(RepairStrategy):
    """
    Removes rows from a PySpark DataFrame based on anomalies detected using a regex pattern.

    This strategy leverages a detection strategy to identify rows where the specified
    regex pattern matches. If the detection results column (`new_column`) does not exist
    in the DataFrame, the detection strategy is applied to generate it. Rows flagged as
    anomalies in the detection results column are removed.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.

        regex_factory_cls (Type[RegexFactory], optional): The regex factory class to use.
            Defaults to `RegexFactory`.
        detect_strategy (Type[RegexDetectStrategy], optional): The detection strategy
            class to use for anomaly detection. Defaults to `RegexDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the detection strategy.

    Methods:
        repair(data: DataFrame) -> DataFrame:
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

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes rows from the PySpark DataFrame where anomalies are detected.

        If the detection results column (`new_column`) is not present in the DataFrame,
        the associated detection strategy is applied to generate it. Rows flagged as
        anomalies in the detection results column are then filtered out.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the column specified for detection does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data


# ------------------------------------------------------------------------------------------------ #
class RegexThresholdDetectStrategy(DetectStrategy):
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
        detect(data: DataFrame) -> DataFrame:
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

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies in the specified column based on a dynamic threshold.

        Args:
            data (DataFrame): The input PySpark DataFrame to analyze.

        Returns:
            DataFrame: The DataFrame with a new column containing detection results.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the regex pattern is invalid or cannot be applied.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Retrieve the regex pattern from the factory
        regex_info = self._regex_factory.get_regex(
            pattern=self._pattern, **self._kwargs
        )

        try:
            # Apply regex to extract matches
            data = data.withColumn(
                "matches",
                F.expr(f"regexp_extract_all({self._column}, '({regex_info.pattern})')"),
            )
        except Exception as e:
            raise ValueError(f"Failed to apply regex pattern: {self._pattern}\n{e}")

        # Count the number of matches
        data = data.withColumn("match_count", F.size(F.col("matches")))

        if self._threshold_type == "count":
            # Detect anomalies based on match count
            data = data.withColumn(
                self._new_column, F.col("match_count") > self._threshold
            )
        elif self._threshold_type == "proportion":
            # Detect anomalies based on proportion
            if self._unit == "word":
                unit_count = F.size(F.split(F.col(self._column), "\\s+"))
            elif self._unit == "character":
                unit_count = F.length(F.col(self._column))
            else:
                raise ValueError(f"Unsupported unit: {self._unit}")

            # Compute proportion and detect anomalies
            data = data.withColumn(
                "proportion",
                (F.col("match_count") / unit_count).cast(DoubleType()),
            )
            data = data.withColumn(
                self._new_column, F.col("proportion") > self._threshold
            )

        # Drop intermediate columns
        data = data.drop("matches", "match_count", "proportion", "unit_count")

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
        repair(data: DataFrame) -> DataFrame:
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

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes rows from the DataFrame where anomalies are detected.

        If the detection results column (`new_column`) is not present in the DataFrame,
        the associated threshold-based detection strategy is applied to generate it. Rows
        flagged as anomalies in the detection results column are then filtered out.

        Args:
            data (DataFrame): The input DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with rows containing anomalies removed.

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
        return data.filter(~F.col(self._new_column))


# ------------------------------------------------------------------------------------------------ #
class CustomRemoveStrategy(RepairStrategy):
    """
    Removes rows from a PySpark DataFrame based on anomalies detected.

    This strategy leverages a detection strategy for detection. If the detection results
    column (`new_column`) does not existi n the DataFrame, the detection strategy is
    applied to generate it. Rows flagged as anomalies in the detection results column are removed.

    Args:
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
        detect_strategy (Type[DetectStrategy], optional): The detection strategy
            class to use for anomaly detection. Defaults to `RegexDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the detection strategy.

    Methods:
        repair(data: DataFrame) -> DataFrame:
            Removes rows from the DataFrame where anomalies are detected.
    """

    def __init__(
        self,
        column: str,
        new_column: str = None,
        detect_strategy: Type[DetectStrategy] = RegexDetectStrategy,
        **kwargs,
    ) -> None:
        self._column = column
        self._new_column = new_column
        self._detect_strategy = detect_strategy
        self._kwargs = kwargs

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes rows from the PySpark DataFrame where anomalies are detected.

        If the detection results column (`new_column`) is not present in the DataFrame,
        the associated detection strategy is applied to generate it. Rows flagged as
        anomalies in the detection results column are then filtered out.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the column specified for detection does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))
        return data


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
        repair(data: DataFrame) -> DataFrame:
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

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies and applies custom repair logic.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the specified detection strategy is used to generate it. The `repair_text` method
        is then applied to rows where anomalies are flagged.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with repaired text in the specified column.

        Raises:
            KeyError: If the specified column does not exist in the DataFrame.
            ValueError: If the detection strategy fails to process the data.
        """
        # Ensure the column exists
        if self._column not in data.columns:
            raise KeyError(f"Column '{self._column}' does not exist in the DataFrame.")

        # Apply detection strategy if the detection column does not exist
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data)

        # Define a UDF for the custom repair logic
        repair_udf = udf(self.repair_text, StringType())

        # Apply the repair logic to rows flagged as anomalies
        data = data.withColumn(
            self._column,
            F.when(F.col(self._new_column), repair_udf(F.col(self._column))).otherwise(
                F.col(self._column)
            ),
        )

        return data

    @staticmethod
    def repair_text(text: str) -> str:
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
    `CustomRegexRepairStrategy` and defines the specific repair logic in the `repair_text` method.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        replacement (str, optional): A placeholder argument inherited from `CustomRegexRepairStrategy`.
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
    def repair_text(text: str) -> str:
        """
        Removes accents and diacritics from a given text string.

        Args:
            text (str): The input string with potential accented characters.

        Returns:
            str: The text with accents and diacritics removed.
        """
        if not text:
            return text
        # Decompose characters into base and diacritics, then remove diacritics
        text_normalized = unicodedata.normalize("NFD", text)
        text_without_accents = "".join(
            char for char in text_normalized if unicodedata.category(char) != "Mn"
        )
        return text_without_accents

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies and repairs text by removing accents and diacritics.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the associated detection strategy is used to generate it. Text repair is then
        applied to rows where anomalies are flagged.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with repaired text in the specified column.
        """
        # Ensure the detection column exists
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data)

        # Define a UDF for the repair logic
        repair_udf = udf(self.repair_text, StringType())

        # Apply the UDF to rows flagged as anomalies
        data = data.withColumn(
            self._column,
            F.when(F.col(self._new_column), repair_udf(F.col(self._column))).otherwise(
                F.col(self._column)
            ),
        )

        return data


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
    def repair_text(text: str) -> str:
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
        if not text:
            return text
        normalized_text = unicodedata.normalize("NFKD", text)
        ascii_text = normalized_text.encode("ascii", "ignore").decode("ascii")
        return ascii_text

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Detects anomalies and repairs text by removing non-ASCII characters.

        If the detection results column (`new_column`) does not exist in the DataFrame,
        the associated detection strategy is used to generate it. Text repair is then
        applied to rows where anomalies are flagged.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with repaired text in the specified column.
        """
        # Ensure the detection column exists
        if self._new_column not in data.columns:
            strategy = self._detect_strategy(
                pattern=self._pattern,
                column=self._column,
                new_column=self._new_column,
                **self._kwargs,
            )
            data = strategy.detect(data)

        # Define a UDF for the repair logic
        repair_udf = udf(self.repair_text, StringType())

        # Apply the repair logic to rows flagged as anomalies
        data = data.withColumn(
            self._column,
            F.when(F.col(self._new_column), repair_udf(F.col(self._column))).otherwise(
                F.col(self._column)
            ),
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                  NON-ENGLISH  DETCTION AND REPAIR                                #
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
class NonEnglishDetectStrategy(DetectStrategy):
    """Detect Non-English in text.

    A PySpark task class for detecting or removing non-English text in a DataFrame column.
    The class provides functionality to either flag non-English text using FastText and Lingua
    language detection libraries or remove rows where the text is non-English.

    Args:
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        **kwargs: Additional keyword arguments passed to the parent class.

    Methods:
        repair_text(text: str) -> str:
            Converts the text to ASCII by removing or replacing non-ASCII characters.
    """

    def __init__(
        self,
        column: str,
        new_column: str = None,
        **kwargs,
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
class NonEnglishRemovalStrategy(CustomRemoveStrategy):
    """
    A strategy for removing rows containing non-English text.

    This strategy detects non-English text in a specified column using a detection
    strategy (defaulting to `NonEnglishDetectStrategy`) and removes rows flagged
    as containing non-English text.

    Args:
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        detect_strategy (Type[DetectStrategy], optional): The detection strategy class
            to use for identifying non-English text. Defaults to `NonEnglishDetectStrategy`.
        **kwargs: Additional keyword arguments passed to the parent class or detection strategy.

    Methods:
        Inherits methods from `CustomRemoveStrategy`, which include functionality
        for applying the detection strategy and removing rows flagged as anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str = None,
        detect_strategy: Type[DetectStrategy] = NonEnglishDetectStrategy,
        **kwargs,
    ) -> None:
        """
        Initializes the NonEnglishRemovalStrategy.

        Args:
            column (str): The name of the column to evaluate.
            new_column (str, optional): The name of the column to store detection results.
                Defaults to None, in which case the column name is used.
            detect_strategy (Type[DetectStrategy], optional): The detection strategy class
                to use for identifying non-English text. Defaults to `NonEnglishDetectStrategy`.
            **kwargs: Additional keyword arguments passed to the parent class or detection strategy.
        """
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            **kwargs,
        )
