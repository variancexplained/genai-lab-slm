#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/quality/privacy.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 3rd 2025 12:57:17 am                                                 #
# Modified   : Tuesday January 28th 2025 03:58:17 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Type, Union

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from genailab.flow.dataprep.quality.base import TextAnomalyDetectRepairTask
from genailab.flow.dataprep.quality.strategy.text.distributed import (
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
        replacement: str = "URL",
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
class DetectRemoveURLTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting and removing URLs in text data.

    Whereas DetectOrRepairURLTask detected and masked URLs, inconsistencies between the detection
    and replacement software left obser

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
        replacement: str = "URL",
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

        def repair(self, data: Union[pd.core.frame.DataFrame, pd.DataFrame, DataFrame]) -> Union[pd.core.frame.DataFrame, pd.DataFrame, DataFrame]:
            """
            Repairs the input data by calling the superclass's repair method and then filtering out rows
            where a specified column (_new_column) is marked as True.

            This method first invokes the `repair` method from the parent class, and then removes any rows
            from the dataset where the value of `_new_column` is `True`.

            Args:
                data (Union[pd.core.frame.DataFrame, pd.DataFrame, DataFrame]): The input dataset to be repaired.
                    It can be either a Pandas DataFrame or a PySpark DataFrame.

            Returns:
                Union[pd.core.frame.DataFrame, pd.DataFrame, DataFrame]: The repaired dataset with rows
                where the '_new_column' is `True` removed.
            """

            # Call the parent class's repair method
            data = super().repair(data=data)

            # Run detect on the data to flag residual rows.
            data = super().detect(data=data)

            # Filter out rows where '_new_column' is True
            return data.filter(F.col(self._new_column) == False)
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
        replacement: str = "EMAIL",
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
