#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/dataprep/clean/validity.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:34:56 pm                                             #
# Modified   : Tuesday December 24th 2024 07:45:40 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Type, Union

from discover.flow.task.dataprep.clean.base import (
    CategoricalAnomalyDetectRepairTask,
    DiscreteAnomaly,
    IntervalAnomaly,
    NumericAnomalyDetectRepairTask,
    TextAnomalyDetectRepairTask,
)
from discover.flow.task.dataprep.clean.strategy.categorical import (
    CategoricalStrategyFactory,
)
from discover.flow.task.dataprep.clean.strategy.discrete import DiscreteStrategyFactory
from discover.flow.task.dataprep.clean.strategy.interval import IntervalStrategyFactory
from discover.flow.task.dataprep.clean.strategy.numeric import NumericStrategyFactory
from discover.flow.task.dataprep.clean.strategy.text.distributed import (
    TextStrategyFactory as SparkTextStrategyFactory,
)


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairURLTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing URLs in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting and repairing URLs in text columns, replacing URLs with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where URLs are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of URL detection/repair. Defaults to "contains_url".
        replacement (str, optional): The string that will replace detected URLs during repair. Defaults to "[URL]".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting URLs in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected URLs. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of URL detection/repair.
        replacement (str): The string used to replace detected URLs during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory]): The class for the strategy factory to use.
        detect_strategy (str): The strategy to use for detecting URLs.
        repair_strategy (str): The strategy to use for repairing detected URLs.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "url"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_url",
        replacement: str = "[URL]",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairEmailAddressTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing email addresses in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting and repairing email addresses in text columns, replacing email addresses
    with a specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where email addresses are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of email address detection/repair. Defaults to "contains_email".
        replacement (str, optional): The string that will replace detected email addresses during repair. Defaults to "[EMAIL]".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting email addresses in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected email addresses. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of email address detection/repair.
        replacement (str): The string used to replace detected email addresses during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting email addresses.
        repair_strategy (str): The strategy to use for repairing detected email addresses.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "email"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_email",
        replacement: str = "[EMAIL]",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairPhoneNumberTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing phone numbers in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting and repairing phone numbers in text columns, replacing phone numbers
    with a specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where phone numbers are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of phone number detection/repair. Defaults to "contains_phone".
        replacement (str, optional): The string that will replace detected phone numbers during repair. Defaults to "[PHONE]".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting phone numbers in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected phone numbers. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of phone number detection/repair.
        replacement (str): The string used to replace detected phone numbers during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting phone numbers.
        repair_strategy (str): The strategy to use for repairing detected phone numbers.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "phone"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_phone",
        replacement: str = "[PHONE]",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveSpecialCharsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive special characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting excessive special characters in text columns and replacing them with
    a specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where excessive special characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of special character detection/repair. Defaults to "contains_excessive_special_chars".
        replacement (str, optional): The string that will replace excessive special characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting excessive special characters in the text. Defaults to "regex_threshold".
        repair_strategy (str, optional): The strategy to use for repairing detected excessive special characters. Defaults to "regex_threshold_remove".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to 0.35.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to "proportion".
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to "character".

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of special character detection/repair.
        replacement (str): The string used to replace excessive special characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting excessive special characters.
        repair_strategy (str): The strategy to use for repairing excessive special characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "special_chars"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excessive_special_chars",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonASCIICharsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing non-ASCII characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting non-ASCII characters in text columns and replacing them with a
    specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where non-ASCII characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of non-ASCII character detection/repair. Defaults to "contains_non_ascii_chars".
        replacement (str, optional): The string that will replace non-ASCII characters during repair. Defaults to None (no replacement).
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting non-ASCII characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected non-ASCII characters. Defaults to "non_ascii".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of non-ASCII character detection/repair.
        replacement (str): The string used to replace non-ASCII characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting non-ASCII characters.
        repair_strategy (str): The strategy to use for repairing non-ASCII characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "non_ascii"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_non_ascii_chars",
        replacement: str = None,
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairControlCharsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing control characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting control characters in text columns and replacing them with a
    specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where control characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of control character detection/repair. Defaults to "contains_control_chars".
        replacement (str, optional): The string that will replace control characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting control characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected control characters. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of control character detection/repair.
        replacement (str): The string used to replace control characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting control characters.
        repair_strategy (str): The strategy to use for repairing control characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "control_chars"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_control_chars",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairHTMLCharsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing HTML characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting HTML characters in text columns and replacing them with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where HTML characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of HTML character detection/repair. Defaults to "contains_html".
        replacement (str, optional): The string that will replace HTML characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting HTML characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected HTML characters. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of HTML character detection/repair.
        replacement (str): The string used to replace HTML characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting HTML characters.
        repair_strategy (str): The strategy to use for repairing HTML characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "html"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_html",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairExcessiveWhitespaceTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive whitespace in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting excessive whitespace in text columns and replacing it with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where excessive whitespace is to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of excessive whitespace detection/repair. Defaults to "contains_excessive_whitespace".
        replacement (str, optional): The string that will replace excessive whitespace during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting excessive whitespace in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected excessive whitespace. Defaults to "whitespace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of excessive whitespace detection/repair.
        replacement (str): The string used to replace excessive whitespace during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting excessive whitespace.
        repair_strategy (str): The strategy to use for repairing excessive whitespace.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "whitespace"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excessive_whitespace",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairAccentedCharsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing accented characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for
    detecting accented characters in text columns and replacing them with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where accented characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of accented character detection/repair. Defaults to "contains_accents".
        replacement (str, optional): The string that will replace accented characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting accented characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected accented characters. Defaults to "accent".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of accented character detection/repair.
        replacement (str): The string used to replace accented characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting accented characters.
        repair_strategy (str): The strategy to use for repairing accented characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "accents"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_accents",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairElongationTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing character elongations (repeated characters) in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for detecting
    elongated characters (e.g., repeated letters in a word) and replacing them with a
    specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where elongations are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of elongation detection/repair. Defaults to "contains_elongation".
        replacement (str, optional): The string that will replace elongated characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting elongated characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected elongated characters. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to 4.
        max_elongation (int, optional): The maximum allowable number of consecutive repeated characters. Defaults to 3.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to "count".
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of elongation detection/repair.
        replacement (str): The string used to replace elongated characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting elongated characters.
        repair_strategy (str): The strategy to use for repairing elongated characters.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        max_elongation (int): The maximum allowable number of consecutive repeated characters.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "elongation"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_elongation",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            max_elongation=max_elongation,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedCharactersTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive repeated characters in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for detecting
    excessive repeated characters (e.g., "aaa") and replacing them with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where repeated characters are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of repeated character detection/repair. Defaults to "contains_excess_repeated_characters".
        replacement (str, optional): The string that will replace excessive repeated characters during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting repeated characters in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected repeated characters. Defaults to "regex_replace".
        min_repetitions (int, optional): The minimum number of repetitions of a character to be considered excessive. Defaults to 4.
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of repeated character detection/repair.
        replacement (str): The string used to replace excessive repeated characters during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting repeated characters.
        repair_strategy (str): The strategy to use for repairing repeated characters.
        min_repetitions (int): The minimum number of repetitions of a character to be considered excessive.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "character_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_characters",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedSequenceTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive repeated sequences in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for detecting
    sequences of characters or words that are excessively repeated (e.g., "abcabcabc")
    and replacing them with a specified placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where repeated sequences are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of repeated sequence detection/repair. Defaults to "contains_excess_repeated_sequences".
        replacement (str, optional): The string that will replace excessive repeated sequences during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting repeated sequences in the text. Defaults to "regex_threshold".
        repair_strategy (str, optional): The strategy to use for repairing detected repeated sequences. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to 3.
        length_of_sequence (int, optional): The length of the sequence to check for repetition. Defaults to 3.
        min_repetitions (int, optional): The minimum number of repetitions of a sequence to be considered excessive. Defaults to 3.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to "count".
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of repeated sequence detection/repair.
        replacement (str): The string used to replace excessive repeated sequences during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting repeated sequences.
        repair_strategy (str): The strategy to use for repairing repeated sequences.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        length_of_sequence (int): The length of the sequence to check for repetition.
        min_repetitions (int): The minimum number of repetitions of a sequence to be considered excessive.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "sequence_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_sequences",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
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
class DetectOrRepairRepeatedWordsTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive repeated words in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for detecting
    repeated words (e.g., "hello hello") and replacing them with a specified placeholder
    string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where repeated words are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of repeated word detection/repair. Defaults to "contains_excess_repeated_words".
        replacement (str, optional): The string that will replace excessive repeated words during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting repeated words in the text. Defaults to "regex_threshold".
        repair_strategy (str, optional): The strategy to use for repairing detected repeated words. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to 1.
        min_repetitions (int, optional): The minimum number of repetitions of a word to be considered excessive. Defaults to 3.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to "count".
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of repeated word detection/repair.
        replacement (str): The string used to replace excessive repeated words during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting repeated words.
        repair_strategy (str): The strategy to use for repairing repeated words.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        min_repetitions (int): The minimum number of repetitions of a word to be considered excessive.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    __PATTERN = "word_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_words",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRepeatedPhraseTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing excessive repeated phrases in text data.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for detecting
    repeated phrases (e.g., "hello world hello world") and replacing them with a specified
    placeholder string during the repair process.

    Args:
        column (str, optional): The name of the column containing the text data where repeated phrases are to be detected. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of repeated phrase detection/repair. Defaults to "contains_excess_repeated_phrases".
        replacement (str, optional): The string that will replace excessive repeated phrases during repair. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting repeated phrases in the text. Defaults to "regex_threshold".
        repair_strategy (str, optional): The strategy to use for repairing detected repeated phrases. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to 3.
        min_repetitions (int, optional): The minimum number of repetitions of a phrase to be considered excessive. Defaults to 3.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to "count".
        unit (Literal["phrase", "character"], optional): Specifies whether to apply the threshold to phrases or characters. Defaults to None.

    Attributes:
        column (str): The name of the column containing the text data.
        new_column (str): The name of the new column that will store the results of repeated phrase detection/repair.
        replacement (str): The string used to replace excessive repeated phrases during repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        detect_strategy (str): The strategy to use for detecting repeated phrases.
        repair_strategy (str): The strategy to use for repairing repeated phrases.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        min_repetitions (int): The minimum number of repetitions of a phrase to be considered excessive.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to phrases or characters.
    """

    __PATTERN = "phrase_repetition"

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_excess_repeated_phrases",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            min_repetitions=min_repetitions,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairGibberishTask(NumericAnomalyDetectRepairTask):
    """
    Class for detecting or repairing gibberish in numeric data based on a threshold.

    This class extends the `NumericAnomalyDetectRepairTask` class and provides functionality for detecting
    gibberish in numeric data (e.g., perplexity scores) based on a threshold, and optionally
    repairing the detected anomalies by setting them to a default value.

    Args:
        column (str, optional): The name of the column containing numeric data to check for gibberish. Defaults to "pa_perplexity".
        new_column (str, optional): The name of the new column that will store the results of gibberish detection/repair. Defaults to "contains_gibberish".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        threshold (float, optional): The threshold value for gibberish detection. Defaults to 0.5.
        relative_error (float, optional): The relative error used for threshold detection. Defaults to 0.001.
        detect_less_than_threshold (bool, optional): Whether to detect gibberish values less than the threshold. Defaults to True.
        detect_strategy (str, optional): The strategy to use for detecting gibberish in the numeric data. Defaults to "threshold".
        repair_strategy (str, optional): The strategy to use for repairing detected gibberish values. Defaults to "threshold".

    Attributes:
        column (str): The name of the column containing numeric data.
        new_column (str): The name of the new column to store the results of gibberish detection/repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[SparkTextStrategyFactory], optional): The class for the strategy factory to use. Defaults to `SparkTextStrategyFactory`.
        threshold (float): The threshold value for gibberish detection.
        relative_error (float): The relative error for threshold detection.
        detect_less_than_threshold (bool): Flag indicating whether to detect values less than the threshold.
        detect_strategy (str): The strategy for detecting gibberish in numeric data.
        repair_strategy (str): The strategy for repairing detected gibberish values.
    """

    def __init__(
        self,
        column: str = "pa_perplexity",
        new_column: str = "contains_gibberish",
        mode: str = "detect",
        strategy_factory_cls: Type[NumericStrategyFactory] = NumericStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairCategoryAnomalyTask(CategoricalAnomalyDetectRepairTask):
    """
    Class for detecting or repairing category anomalies in categorical data.

    This class extends the `CategoricalAnomalyDetectRepairTask` class and provides functionality for detecting
    and optionally repairing anomalies in categorical data based on a list of valid categories.

    Args:
        column (str): The name of the column containing categorical data to check for anomalies.
        new_column (str): The name of the new column that will store the results of category anomaly detection/repair.
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[CategoricalStrategyFactory], optional): The class for the strategy factory to use. Defaults to `CategoricalStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting category anomalies. Defaults to "categorical".
        repair_strategy (str, optional): The strategy to use for repairing detected category anomalies. Defaults to "categorical".
        valid_categories (list, optional): A list of valid categories to use for anomaly detection. Defaults to None.

    Attributes:
        column (str): The name of the column containing categorical data.
        new_column (str): The name of the new column to store the results of category anomaly detection/repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[CategoricalStrategyFactory]): The class for the strategy factory to use.
        detect_strategy (str): The strategy for detecting category anomalies.
        repair_strategy (str): The strategy for repairing category anomalies.
        valid_categories (list): The list of valid categories to validate against.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        strategy_factory_cls: Type[
            CategoricalStrategyFactory
        ] = CategoricalStrategyFactory,
        detect_strategy: str = "categorical",
        repair_strategy: str = "categorical",
        valid_categories: list = None,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            strategy_factory_cls=strategy_factory_cls,
            valid_categories=valid_categories,
            detect_strategy="categorical",
            repair_strategy="categorical",
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairRatingAnomalyTask(DiscreteAnomaly):
    """
    Class for detecting or repairing rating anomalies in discrete data.

    This class extends the `DiscreteAnomaly` class and provides functionality for detecting
    and optionally repairing anomalies in rating data, based on a specified valid range.

    Args:
        column (str): The name of the column containing the rating data to check for anomalies.
        new_column (str): The name of the new column that will store the results of rating anomaly detection/repair.
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (Type[DiscreteStrategyFactory], optional): The class for the strategy factory to use. Defaults to `DiscreteStrategyFactory`.
        detect_strategy (str, optional): The strategy to use for detecting rating anomalies. Defaults to "range".
        repair_strategy (str, optional): The strategy to use for repairing detected rating anomalies. Defaults to "range".
        range_min (int, optional): The minimum valid value for the rating. Defaults to 1.
        range_max (int, optional): The maximum valid value for the rating. Defaults to 5.

    Attributes:
        column (str): The name of the column containing rating data.
        new_column (str): The name of the new column to store the results of rating anomaly detection/repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (Type[DiscreteStrategyFactory]): The class for the strategy factory to use.
        detect_strategy (str): The strategy for detecting rating anomalies.
        repair_strategy (str): The strategy for repairing rating anomalies.
        range_min (int): The minimum valid rating value.
        range_max (int): The maximum valid rating value.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        strategy_factory_cls: Type[DiscreteStrategyFactory] = DiscreteStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            range_min=range_min,
            range_max=range_max,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairReviewDateAnomalyTask(IntervalAnomaly):
    """
    Class for detecting or repairing review date anomalies in interval data.

    This class extends the `IntervalAnomaly` class and provides functionality for detecting
    and optionally repairing anomalies in review dates based on a specified valid date range.

    Args:
        column (str): The name of the column containing the review date data to check for anomalies.
        new_column (str): The name of the new column that will store the results of review date anomaly detection/repair.
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (str, optional): The ID of the strategy factory to use. Defaults to "interval".
        detect_strategy (str, optional): The strategy to use for detecting review date anomalies. Defaults to "date_range".
        repair_strategy (str, optional): The strategy to use for repairing detected review date anomalies. Defaults to "date_range".
        range_min (int, optional): The minimum valid year for the review date. Defaults to 2020.
        range_max (int, optional): The maximum valid year for the review date. Defaults to 2023.
        range_type (str, optional): The type of date range to check against ("year", "month", or "date"). Defaults to "year".

    Attributes:
        column (str): The name of the column containing review date data.
        new_column (str): The name of the new column to store the results of review date anomaly detection/repair.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (str): The ID of the strategy factory to use.
        detect_strategy (str): The strategy for detecting review date anomalies.
        repair_strategy (str): The strategy for repairing review date anomalies.
        range_min (int): The minimum valid year for the review date.
        range_max (int): The maximum valid year for the review date.
        range_type (str): The type of date range to check against ("year", "month", or "date").
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str = "detect",
        strategy_factory_cls: Type[IntervalStrategyFactory] = IntervalStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=detect_strategy,
            range_min=range_min,
            range_max=range_max,
            range_type=range_type,
            **kwargs,
        )
