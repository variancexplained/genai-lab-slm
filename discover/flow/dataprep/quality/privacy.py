#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/quality/privacy.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 3rd 2025 12:57:17 am                                                 #
# Modified   : Friday January 3rd 2025 12:59:56 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Type, Union

from discover.flow.dataprep.quality.base import TextAnomalyDetectRepairTask
from discover.flow.dataprep.quality.strategy.text.distributed import (
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
