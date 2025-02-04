#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/quality/strategy/text/distributed.py                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 03:13:48 am                                             #
# Modified   : Tuesday February 4th 2025 02:05:44 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import re
import unicodedata
from typing import Dict, Literal, Type, Union

import chardet
import fasttext
import pandas as pd
from lingua import Language, LanguageDetectorBuilder
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import BooleanType, DoubleType, StringType

from genailab.flow.dataprep.quality.strategy.factory import (
    DetectStrategy,
    RepairStrategy,
    StrategyFactory,
)
from genailab.flow.dataprep.quality.strategy.text import SPECIAL_ACCENT_MAP
from genailab.flow.dataprep.quality.strategy.text.pattern import RegexFactory

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
    def detect_strategies(self) -> Dict[str, Type[DetectStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex": RegexDetectStrategy,
            "regex_threshold": RegexThresholdDetectStrategy,
            "non_english": NonEnglishDetectStrategy,
            "short_review": ShortReviewDetectStrategy,
        }

    @property
    def repair_strategies(self) -> Dict[str, Type[RepairStrategy]]:
        """Returns a dictionary of detect strategies"""
        return {
            "regex_replace": RegexReplaceStrategy,
            "regex_remove": RegexRemoveStrategy,
            "regex_threshold_remove": RegexThresholdRemoveStrategy,
            "accent": AccentRepairStrategy,
            "non_ascii": NonAsciiRepairStrategy,
            "non_english": NonEnglishRemovalStrategy,
            "whitespace": ExcessWhitespaceRepairStrategy,
            "short_review": ShortReviewRemovalStrategy,
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
        super().__init__()
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
        **kwargs,
    ) -> None:
        super().__init__()
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
            data = data.withColumn(
                self._column,
                F.when(
                    F.col(self._column).rlike(regex_info.pattern),  # Detect match
                    F.regexp_replace(F.col(self._column), regex_info.pattern, replacement)  # Apply replacement
                ).otherwise(F.col(self._column))  # Leave untouched if no match
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
        super().__init__()
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
        super().__init__()
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

        # Re-apply detection strategy
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
class RemoveDetectedAnomalyStrategy(RepairStrategy):
    """
    Removes rows from a PySpark DataFrame based on anomalies detected.

    Args:
        indicator_column (str): The column containing the binary anomaly indicator.
        **kwargs: Additional keyword arguments passed to the detection strategy.

    Methods:
        repair(data: DataFrame) -> DataFrame:
            Removes rows from the DataFrame where anomalies are detected.
    """

    def __init__(
        self,
        indicator_column: str,
        **kwargs,
    ) -> None:
        self._indicator_column = indicator_column
        self._kwargs = kwargs

    def repair(self, data: DataFrame) -> DataFrame:
        """
        Removes rows from the PySpark DataFrame where anomalies are detected.

        Args:
            data (DataFrame): The input PySpark DataFrame to process.

        Returns:
            DataFrame: A new DataFrame with rows containing anomalies removed.

        Raises:
            KeyError: If the column specified for detection does not exist in the DataFrame.

        """
        # Ensure the column exists
        if self._indicator_column not in data.columns:
            raise KeyError(
                f"Column '{self._indicator_column}' does not exist in the DataFrame."
            )

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._indicator_column))
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

        # Apply detection strategy
        strategy = self._detect_strategy(
            pattern=self._pattern,
            column=self._column,
            new_column=self._new_column,
            **self._kwargs,
        )
        data = strategy.detect(data)

        # Apply the repair logic to rows flagged as anomalies
        data = data.withColumn(
            self._column,
            F.when(F.col(self._new_column), self.repair_text(F.col(self._column))).otherwise(
                F.col(self._column)
            ),
        )

        return data


    @pandas_udf(StringType())
    def repair_text(texts: pd.Series) -> pd.Series:
        """
        Abstract method to define custom text repair logic as a Pandas UDF.

        Subclasses must implement this method to specify how text in rows flagged
        as anomalies should be repaired.

        Args:
            texts (pd.Series): A Pandas Series of input texts to repair.

        Returns:
            pd.Series: The repaired texts as a Pandas Series.
        """
        pass


# ------------------------------------------------------------------------------------------------ #
class ExcessWhitespaceRepairStrategy(CustomRegexRepairStrategy):

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
        self._kwargs = kwargs


    @pandas_udf(StringType())
    def repair_text(texts: pd.Series) -> pd.Series:
        """
        Cleans excessive whitespace from text, including non-breaking spaces
        and other invisible characters.

        Args:
            texts (pd.Series): A Pandas Series of input texts to clean.

        Returns:
            pd.Series: Cleaned text with normalized whitespace.
        """
        return texts.str.replace(r"[\s\u00A0\u200B]+", " ", regex=True).str.strip()

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

        # Apply the UDF to rows flagged as anomalies
        data = data.withColumn(self._column, self.repair_text(F.col(self._column)))

        return data


# ------------------------------------------------------------------------------------------------ #
class AccentRepairStrategy(CustomRegexRepairStrategy):
    """
    A strategy for repairing text by removing accents and diacritics.

    This strategy detects anomalies based on a regex pattern and repairs the text
    by removing accents and diacritical marks from characters. It extends the
    `CustomRegexRepairStrategy` and defines the specific repair logic in the `repair_text_pandas` method.

    Args:
        pattern (str): The regex pattern used to detect anomalies.
        column (str): The name of the column to evaluate.
        new_column (str, optional): The name of the column to store detection results.
            Defaults to None, in which case the column name is used.
        replacement (str, optional): A placeholder argument inherited from `CustomRegexRepairStrategy`.
            Not used in this class but included for compatibility.
        **kwargs: Additional keyword arguments passed to the parent class.

    Methods:
        repair_text_pandas(series: pd.Series) -> pd.Series:
            Removes accents and diacritics from a pandas Series of text strings, including handling special cases.
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
        self._kwargs = kwargs


    @pandas_udf(StringType())
    def repair_text(series: pd.Series) -> pd.Series:
        """
        Removes accents and diacritics from a pandas Series of text strings, including handling special cases.

        Args:
            series (pd.Series): A pandas Series of text strings.

        Returns:
            pd.Series: A pandas Series with accents and diacritical marks removed.
        """

        def remove_accents(text: str) -> str:
            if not text:
                return text

            # Normalize to decomposed form (NFD)
            text_normalized = unicodedata.normalize("NFD", text)

            # Remove diacritics (category 'Mn' means "Mark, Nonspacing")
            text_without_accents = "".join(
                char for char in text_normalized if unicodedata.category(char) != "Mn"
            )

            # Create a translation table from SPECIAL_ACCENT_MAP
            translation_table = str.maketrans(SPECIAL_ACCENT_MAP)

            # Apply translation using the table
            text_without_accents = text_without_accents.translate(translation_table)

            # Normalize back to composed form (NFC)
            return unicodedata.normalize("NFC", text_without_accents)

        return series.apply(remove_accents)

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

        # Apply the UDF to rows flagged as anomalies
        data = data.withColumn(self._column, self.repair_text(F.col(self._column)))

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

    @pandas_udf(StringType())
    def repair_text(series: pd.Series) -> pd.Series:
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
        if not series:
            return series

        return series.apply(lambda text: unicodedata.normalize("NFKD", text)
                                        .encode("ascii", "ignore")
                                        .decode("ascii") if isinstance(text, str) else text)

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

        # Apply the repair logic to rows flagged as anomalies
        data = data.withColumn(
            self._column,
            F.when(F.col(self._new_column), self.repair_text(F.col(self._column))).otherwise(
                F.col(self._column)
            ),
        )

        return data


# ------------------------------------------------------------------------------------------------ #
#                                  NON-ENGLISH  DETCTION AND REPAIR                                #
# ------------------------------------------------------------------------------------------------ #
def lm_fasttext(text: str) -> bool:
    """
    Detects if the given text is non-English using the FastText model.

    This function uses the FastText model to predict the language of the input text
    and classifies it as non-English if the predicted label is not `__label__en`.
    It returns a boolean indicating whether the text is non-English.

    Args:
        text (str): The input text to be analyzed.

    Returns:
        bool: `True` if the text is non-English, `False` if it is English.

    Raises:
        Exception: If there is an error during FastText language detection, it returns `False` and logs the error.
    """
    try:
        return fasttext_model.predict(text)[0][0] != "__label__en" if isinstance(text, str) else False
    except Exception as e:
        print(f"Error in FastText language detection: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #

def lm_lingua(text: str) -> bool:
    """
    Detects if the given text is non-English using the Lingua language detection library.

    This function uses the Lingua detector to check if the input text is in English or not.
    It returns `True` if the text is not in English and `False` if it is.

    Args:
        text (str): The input text to be analyzed.

    Returns:
        bool: `True` if the text is non-English, `False` if it is English.

    Raises:
        Exception: If there is an error during Lingua language detection, it returns `False` and logs the error.
    """
    try:
        return detector.detect_language_of(text) != Language.ENGLISH if isinstance(text, str) else False
    except Exception as e:
        print(f"Error in Lingua re-evaluation: {e}")
        return False
# ------------------------------------------------------------------------------------------------ #
def fast_non_english(text):
    """
    Fast non-English detection (Mandarin, Arabic, Russian, Greek, etc.).
    Prioritizes speed; some false negatives for Latin languages are acceptable.
    """
    if not text:
        return False  # Treat empty text as English

    try:
        encoding_result = chardet.detect(text.encode())
        encoding = encoding_result['encoding']
        confidence = encoding_result['confidence']

        if confidence < 0.7:  # Lowered confidence threshold slightly
            return False  # Treat as English

        # Mandarin Check
        if encoding in ('GBK', 'GB2312', 'UTF-8') and re.search(r'[\u4e00-\u9fff]', text):
            return True

        # Arabic Check
        if encoding in ('Arabic', 'UTF-8', 'windows-1256') and re.search(r'[\u0600-\u06FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF]', text):
            return True

        # Russian Check (Cyrillic)
        if encoding in ('UTF-8', 'Windows-1251', 'KOI8-R') and re.search(r'[\u0400-\u052F]', text):
            return True

        # Greek Check
        if encoding in ('UTF-8', 'ISO-8859-7') and re.search(r'[\u0370-\u03FF]', text):
            return True

        return False  # Default: English (includes Latin languages)

    except Exception as e:
        return False  # Default to English on error
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
        fast: bool = True,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._fast = fast

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Detects non-English text in the specified column using FastText and Lingua.

        Args:
            data (DataFrame): A PySpark DataFrame containing the data to be processed.

        Returns:
            DataFrame: A PySpark DataFrame with an additional boolean column indicating
            whether the text is non-English.
        """
        @pandas_udf(BooleanType())
        def fastext_udf(texts: pd.Series) -> pd.Series:
            """
            Applies FastText language detection to each row of the input text column.

            This function strips leading/trailing spaces from the input text and then
            applies FastText to predict whether the text is in English or non-English.
            It returns a boolean Series indicating whether each text is non-English.

            Args:
                texts (pd.Series): A Pandas Series of input texts to evaluate.

            Returns:
                pd.Series: A Pandas Series of booleans, where `True` indicates non-English text,
                and `False` indicates English text.
            """
            return texts.str.strip().map(lm_fasttext)

        @pandas_udf(BooleanType())
        def lingua_udf(texts: pd.Series) -> pd.Series:
            """
            Applies Lingua language detection to each row of the input text column.

            This function strips leading/trailing spaces from the input text and then
            applies Lingua to check if the text is in English or not. It returns a boolean
            Series indicating whether each text is non-English.

            Args:
                texts (pd.Series): A Pandas Series of input texts to evaluate.

            Returns:
                pd.Series: A Pandas Series of booleans, where `True` indicates non-English text,
                and `False` indicates English text.
            """
            return texts.str.strip().map(lm_lingua)

        @pandas_udf(BooleanType())
        def fast_non_english_udf(texts: pd.Series) -> pd.Series:
            """
            Applies fast non-english function to each row of the input text column.

            This function strips leading/trailing spaces from the input text and then
            applies FastText to predict whether the text is in English or non-English.
            It returns a boolean Series indicating whether each text is non-English.

            Args:
                texts (pd.Series): A Pandas Series of input texts to evaluate.

            Returns:
                pd.Series: A Pandas Series of booleans, where `True` indicates non-English text,
                and `False` indicates English text.
            """
            return texts.str.strip().map(fast_non_english)

        if self._fast:
            data = data.withColumn(self._new_column, fast_non_english_udf(F.col(self._column)))
        else:
            # Apply the FastText language detection
            data = data.withColumn(self._new_column, fastext_udf(F.col(self._column)))

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
class NonEnglishRemovalStrategy(RepairStrategy):
    """
    A strategy for removing rows containing non-English flagged during the detection stage.

    This strategy removes observations previously flagged as containing non-English text
    in a specified anomaly indicator column.

    Args:
        indicator_column (str): The name of anomaly indicator column.

    Methods:
        Inherits methods from `RemoveDetectedAnomalyStrategy`, which removes observations
        containing Non-English text.
    """

    def __init__(
        self,
        indicator_column: str,
        **kwargs,
    ) -> None:
        self._indicator_column = indicator_column

    def repair(self, data: DataFrame) -> DataFrame:

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._indicator_column))

        return data


# ------------------------------------------------------------------------------------------------ #
#                                       SHORT REVIEWS                                              #
# ------------------------------------------------------------------------------------------------ #
class ShortReviewDetectStrategy(DetectStrategy):
    """
    A strategy to detect short or long reviews based on word count.

    This strategy adds a boolean column to a PySpark DataFrame, indicating whether the
    length of text content in a specified column (in words) is less than or greater than
    a given threshold.

    Args:
        column (str): The name of the column containing text data to analyze.
        new_column (str, optional): The name of the new column to be added. If not provided,
            defaults to `<column>_short_review_detected`.
        threshold (int, optional): The word count threshold for determining short or long reviews. Defaults to 10.
        detect_less_than_threshold (bool, optional): If True, flags rows with word count less
            than the threshold. If False, flags rows with word count greater than the threshold. Defaults to True.
        **kwargs: Additional arguments for extended functionality.

    Methods:
        detect(data: DataFrame) -> DataFrame:
            Adds a boolean column to the input DataFrame, flagging rows based on word count.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        threshold: int = 10,
        detect_less_than_threshold: bool = True,
        **kwargs,
    ) -> None:
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._detect_less_than_threshold = detect_less_than_threshold

    def detect(self, data: DataFrame) -> DataFrame:
        """
        Adds a boolean column to the DataFrame indicating if the text content's word count
        in the specified column is less than or greater than the threshold.

        Args:
            data (DataFrame): Input PySpark DataFrame containing the column to analyze.

        Returns:
            DataFrame: A PySpark DataFrame with an additional boolean column indicating
            whether the word count is less than or greater than the threshold.

        Raises:
            ValueError: If the specified column does not exist in the DataFrame.
        """
        # Add a column with the length of the review in words
        data = data.withColumn(
            "review_word_count", F.size(F.split(F.col(self._column), r"\s+"))
        )

        # Add the detection column based on the threshold
        if self._detect_less_than_threshold:
            data = data.withColumn(
                self._new_column,
                F.when(F.col("review_word_count") < self._threshold, True).otherwise(False),
            )
        else:
            data = data.withColumn(
                self._new_column,
                F.when(F.col("review_word_count") > self._threshold, True).otherwise(False),
            )

        # Drop the intermediate column if desired
        data = data.drop("review_word_count")

        return data


# ------------------------------------------------------------------------------------------------ #
class ShortReviewRemovalStrategy(RepairStrategy):
    """A strategy for removing short reviews from a DataFrame.

    This class defines a repair strategy that identifies and removes reviews shorter than a specified threshold.
    It leverages a detection strategy to flag reviews based on their length and filters out the flagged rows.

    Args:
        column (str): The name of the column containing the reviews to be processed.
        new_column (str, optional): The name of the column to store detection results. If None, the detection
            results will replace the existing column. Defaults to None.
        threshold (int, optional): The minimum length of reviews to keep. Reviews shorter than this threshold
            will be flagged for removal. Defaults to 10.
        detect_less_than_threshold (bool, optional): Flag indicating whether to detect reviews shorter than
            the threshold. If False, reviews longer than the threshold are flagged. Defaults to True.
        detect_strategy (Type[ShortReviewDetectStrategy], optional): The detection strategy class used to
            identify short reviews. Defaults to ShortReviewDetectStrategy.
        **kwargs: Additional keyword arguments passed to the base RepairStrategy class.

    Attributes:
        _column (str): The name of the column containing the reviews to be processed.
        _new_column (str): The name of the column to store detection results.
        _threshold (int): The minimum length of reviews to keep.
        _detect_less_than_threshold (bool): Indicates whether to detect reviews shorter than the threshold.
        _detect_strategy (Type[ShortReviewDetectStrategy]): The detection strategy class used to identify
            short reviews.

    Methods:
        repair(data: DataFrame) -> DataFrame: Applies the repair strategy to the given DataFrame by detecting
            and removing short reviews.
    """

    def __init__(
        self,
        column: str,
        new_column: str = None,
        threshold: int = 10,
        clean_less_than_threshold: bool = True,
        detect_strategy: Type[ShortReviewDetectStrategy] = ShortReviewDetectStrategy,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._new_column = new_column
        self._threshold = threshold
        self._clean_less_than_threshold = clean_less_than_threshold
        self._detect_strategy = detect_strategy

    def repair(self, data: DataFrame) -> DataFrame:
        """Applies the repair strategy to the given DataFrame by detecting and removing short reviews.

        Args:
            data (DataFrame): The input DataFrame containing the reviews.

        Returns:
            DataFrame: The DataFrame with short reviews removed.
        """
        # Construct the anomaly detection strategy
        detect_strategy = self._detect_strategy(
            column=self._column,
            new_column=self._new_column,
            threshold=self._threshold,
            detect_less_than_threshold=self._clean_less_than_threshold,
        )

        # Execute anomaly detection
        data = detect_strategy.detect(data=data)

        # Filter out rows where anomalies are detected
        data = data.filter(~F.col(self._new_column))

        return data
