#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/dimension/validity.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:34:56 pm                                             #
# Modified   : Sunday November 24th 2024 10:54:33 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Union

from discover.flow.task.clean.dimension.anomaly import (
    CategoricalAnomaly,
    DiscreteAnomaly,
    IntervalAnomaly,
    NumericAnomaly,
    TextAnomaly,
)


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairURLTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing URLs in a DataFrame column.
    The class provides functionality to either flag the presence of URLs in a new
    column or replace URLs with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variable:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "url"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_url",
        replacement: str = "[URL]",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairEmailAddressTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing email addresses in a DataFrame column.
    The class provides functionality to either flag the presence of email addresses in a
    new column or replace email addresses with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "email"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_email",
        replacement: str = "[EMAIL]",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairPhoneNumberTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing phone numbers in a DataFrame column.
    The class provides functionality to either flag the presence of phone numbers in a
    new column or replace phone numbers with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "phone"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_phone",
        replacement: str = "[PHONE]",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveSpecialCharsTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing phone numbers in a DataFrame column.
    The class provides functionality to either flag the presence of phone numbers in a
    new column or replace phone numbers with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "special_chars"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excessive_special_chars",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex_threshold",
        repair_strategy: str = "regex_threshold_remove",
        threshold: Union[float, int] = 0.35,
        threshold_type: Literal["count", "proportion"] = "proportion",
        unit: Literal["word", "character"] = "character",
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonASCIICharsTask(TextAnomaly):
    """
    A task for detecting or repairing non-ASCII characters in text data.

    This class extends `DetectOrReplaceTask` to provide functionality for either
    detecting or repairing text that contains non-ASCII (Unicode) characters. In
    'detect' mode, it flags text with non-ASCII characters. In 'repair' mode, it
    attempts to normalize the text by removing non-ASCII characters and converting
    it to an ASCII-only format.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "non_ascii"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_non_ascii_chars",
        replacement: str = None,
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "non_ascii",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairControlCharsTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing control characters in a DataFrame column.
    The class provides functionality to either flag the presence of control characters or
    remove/replace them with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "control_chars"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_control_chars",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairHTMLCharsTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing HTML character entities in a DataFrame column.
    The class provides functionality to either flag the presence of HTML character entities
    or remove/replace them with a specified replacement string.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "html"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_html",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveWhitespaceTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing excessive whitespace in a DataFrame column.
    The class provides functionality to either flag rows with excessive whitespace or
    replace multiple whitespace characters with a single space.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "whitespace"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excessive_whitespace",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "whitespace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairAccentedCharsTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing accented and diacritic characters in a DataFrame column.
    The class provides functionality to either flag the presence of accented characters or
    normalize text by removing accents and diacritics.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "accents"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_accents",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "accent",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairElongationTask(TextAnomaly):
    """
    A PySpark task class for detecting or repairing elongated characters in a DataFrame column.
    The class provides functionality to either flag instances of character elongation (e.g., "sooo good")
    or replace them by limiting the repetition of characters to a specified maximum.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "elongation"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_elongation",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = 4,
        max_elongation: int = 3,
        threshold_type: Literal["count", "proportion"] = "count",
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            max_elongation=max_elongation,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedCharactersTask(TextAnomaly):

    __PATTERN = "character_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_characters",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        min_repetitions: int = 4,
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedSequenceTask(TextAnomaly):
    """
    A task for detecting or repairing excessive sequence repetition in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated patterns within the specified text column. The task can
    be configured to either detect or repair these patterns.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "sequence_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_sequences",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex_threshold",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = 3,
        length_of_sequence: int = 3,
        min_repetitions: int = 3,
        threshold_type: Literal["count", "proportion"] = "count",
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            length_of_sequence=length_of_sequence,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedWordsTask(TextAnomaly):
    """
    A task for detecting or repairing excessive word repetition in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated patterns within the specified text column. The task can
    be configured to either detect or repair these patterns.

    Args:

        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["word", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "word" or "character". Defaults to "word".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "word_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_words",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex_threshold",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = 1,
        min_repetitions: int = 3,
        threshold_type: Literal["count", "proportion"] = "count",
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedPhraseTask(TextAnomaly):
    """
    A task for detecting or repairing excessive phrase repetition in text.

    This class inherits from `DetectOrRemoveTask` and uses a regular expression
    to identify repeated patterns within the specified text column. The task can
    be configured to either detect or repair these patterns.

    Args:
        column (str): The name of the column containing text to examine.
        new_column (str): The name of the new column for detection flags.
        replacement (str): The string to replace URLs with in 'repair' mode.
        threshold (float): An optional threshold for detection logic.
        mode (str): The mode of operation, either 'detect' or 'repair'.
        distributed (bool): Whether to use a distributed runtime environment.
        threshold (Union[float, int]): The threshold value for anomaly detection.
        threshold_type (Literal["count", "proportion"]): The type of threshold. Use "count" for
            a fixed number of matches or "proportion" for a relative ratio.
        unit (Literal["phrase", "character"], optional): The unit for proportions when `threshold_type`
            is "proportion". Must be "phrase" or "character". Defaults to "phrase".

    Class Variables:
        __PATTERN (str): Returns the pattern corresponding with a pattern in the PatternFactory.
            See discover/flow/task/clean/strategy/text/pattern.py for a list of valid patterns.
    """

    __PATTERN = "phrase_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_phrases",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex_threshold",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = 3,
        min_repetitions: int = 3,
        threshold_type: Literal["count", "proportion"] = "count",
        unit: Literal["phrase", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
            pattern=self.__PATTERN,
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairGibberishTask(NumericAnomaly):
    """
    A task for detecting or repairing gibberish content based on numeric anomalies.

    This task evaluates numeric data, such as perplexity scores, to detect or repair
    rows flagged as gibberish. It uses percentile-based anomaly detection and repair
    strategies. Users can configure detection thresholds, operation modes, and execution
    (distributed or local).

    Args:
        column (str, optional): The name of the column to evaluate for anomalies.
            Defaults to "pa_perplexity".
        new_column (str, optional): The name of the column to store detection or repair results.
            Defaults to "contains_gibberish".
        mode (str, optional): The operation mode: "detect" for anomaly detection or "repair"
            for anomaly repair. Defaults to "detect".
        distributed (bool, optional): If True, uses distributed strategies; otherwise, uses local strategies.
            Defaults to True.
        threshold (float, optional): The percentile threshold for anomaly detection.
            Defaults to 0.5 (50th percentile, median).
        relative_error (float, optional): The relative error for the `approxQuantile` calculation.
            Smaller values result in more precise thresholds but may increase computation time.
            Defaults to 0.001.
        detect_less_than_threshold (bool, optional): If True, detects values below the threshold.
            If False, detects values above the threshold. Defaults to True.
        detect_strategy (Type[ThresholdPercentileAnomalyDetectStrategy], optional): The detection
            strategy class to use for identifying anomalies. Defaults to `ThresholdPercentileAnomalyDetectStrategy`.
        repair_strategy (Type[ThresholdPercentileAnomalyRepairStrategy], optional): The repair
            strategy class to use for handling anomalies. Defaults to `ThresholdPercentileAnomalyRepairStrategy`.
        **kwargs: Additional keyword arguments for advanced configuration or strategy customization.

    Methods:
        Inherits methods from `NumericAnomaly`, including functionality for applying detection
        and repair strategies to gibberish content.
    """

    def __init__(
        self,
        column: str = "pa_perplexity",
        new_column: str = "contains_gibberish",
        mode: str = "detect",
        distributed: bool = True,
        threshold: float = 0.5,
        relative_error: float = 0.001,
        detect_less_than_threshold: bool = True,
        detect_strategy: str = "threshold",
        repair_strategy: str = "threshold",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairCategoryAnomalyTask(CategoricalAnomaly):
    """
    A task for detecting or repairing anomalies in categorical data.

    This task evaluates a column of categorical data to identify and optionally
    repair values that do not belong to a predefined list of valid categories.
    Users can configure the operation mode ("detect" or "repair") and whether
    to use distributed or local execution.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection or repair results.
            This column will contain `True` for rows with invalid categories and `False` otherwise.
        mode (str, optional): The operation mode: "detect" for anomaly detection or "repair"
            for anomaly repair. Defaults to "detect".
        distributed (bool, optional): If True, uses distributed strategies; otherwise, uses local strategies.
            Defaults to True.
        valid_categories (list, optional): A list of valid categorical values to compare against.
            Defaults to None, which implies no valid categories are defined.
        **kwargs: Additional keyword arguments for advanced configuration or strategy customization.

    Methods:
        Inherits methods from `CategoricalAnomaly`, including functionality for detecting
        and repairing categorical anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "categorical",
        repair_strategy: str = "categorical",
        valid_categories: list = None,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            valid_categories=valid_categories,
            detect_strategy="categorical",
            repair_strategy="categorical",
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRatingAnomalyTask(DiscreteAnomaly):
    """
    Task for detecting or repairing anomalies in rating data.

    This class is designed to handle rating anomalies by detecting values
    that fall outside a specified range or repairing them to conform to
    the specified range. It extends `DiscreteAnomaly` to leverage configurable
    strategies for detection and repair and supports operation in both local
    and distributed environments.

    Args:
        column (str): The name of the column containing rating values to evaluate for anomalies.
        new_column (str): The name of the column to store the results of the operation
            (e.g., anomaly flags or repaired values).
        mode (str, optional): The mode of operation, either `"detect"` for anomaly
            detection or `"repair"` for anomaly repair. Defaults to `"detect"`.
        distributed (bool, optional): Whether the operations should be performed
            in a distributed environment (e.g., with PySpark). Defaults to `True`.
        detect_strategy (str, optional): The detection strategy to identify rating anomalies.
            Defaults to `"range"`.
        repair_strategy (str, optional): The repair strategy to handle rating anomalies.
            Defaults to `"range"`.
        range_min (int, optional): The minimum acceptable rating value. Defaults to `1`.
        range_max (int, optional): The maximum acceptable rating value. Defaults to `5`.
        **kwargs: Additional keyword arguments passed to the parent class.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "range",
        repair_strategy: str = "range",
        range_min: int = 1,
        range_max: int = 5,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=detect_strategy,
            range_min=range_min,
            range_max=range_max,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairReviewDateAnomalyTask(IntervalAnomaly):
    """
    Task for detecting or repairing anomalies in review date data.

    This class is designed to handle review date anomalies by detecting values
    that fall outside a specified date range or repairing them to conform to
    the specified range. It extends `IntervalAnomaly` to leverage configurable
    strategies for detection and repair and supports operation in both local
    and distributed environments.

    Args:
        column (str): The name of the column containing review dates to evaluate for anomalies.
        new_column (str): The name of the column to store the results of the operation
            (e.g., anomaly flags or repaired values).
        mode (str, optional): The mode of operation, either `"detect"` for anomaly
            detection or `"repair"` for anomaly repair. Defaults to `"detect"`.
        distributed (bool, optional): Whether the operations should be performed
            in a distributed environment (e.g., with PySpark). Defaults to `True`.
        detect_strategy (str, optional): The detection strategy to identify review date anomalies.
            Defaults to `"date_range"`.
        repair_strategy (str, optional): The repair strategy to handle review date anomalies.
            Defaults to `"date_range"`.
        range_min (int, optional): The minimum acceptable review date (e.g., year as an integer). Defaults to `2020`.
        range_max (int, optional): The maximum acceptable review date (e.g., year as an integer). Defaults to `2023`.
        range_type: (Literal["year", "month", "date"]): Indicates whether the ranges are years, months, or dates.
        **kwargs: Additional keyword arguments passed to the parent class.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "date_range",
        repair_strategy: str = "date_range",
        range_min: int = 2020,
        range_max: int = 2023,
        range_type: Literal["year", "month", "date"] = "year",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            distributed=distributed,
            detect_strategy=detect_strategy,
            repair_strategy=detect_strategy,
            range_min=range_min,
            range_max=range_max,
            range_type=range_type,
            **kwargs,
        )
