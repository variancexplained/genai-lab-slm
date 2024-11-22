#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/dimension/anomaly.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 05:35:51 pm                                             #
# Modified   : Thursday November 21st 2024 11:16:42 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Type, Union

from discover.flow.task.clean.base.anomaly import Anomaly
from discover.flow.task.clean.strategy.categorical import CategoricalStrategyFactory
from discover.flow.task.clean.strategy.numeric import NumericStrategyFactory
from discover.flow.task.clean.strategy.text.distributed import (
    TextStrategyFactory as DistributedTextStrategyFactory,
)
from discover.flow.task.clean.strategy.text.local import (
    TextStrategyFactory as LocalTextStrategyFactory,
)


# ------------------------------------------------------------------------------------------------ #
class TextAnomaly(Anomaly):
    """
    A class for detecting and repairing text anomalies using various strategies.

    This class allows users to handle text anomalies such as noise, encoding issues, or
    invalid characters using customizable detection and repair strategies. It supports both
    distributed and local execution modes and provides flexibility to configure thresholds,
    patterns, and repair logic.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection or repair results.
            Automatically prefixed for consistency.
        pattern (str, optional): The regex pattern used for detection. Defaults to None.
        replacement (str, optional): The replacement value for the regex replace operation.
            Defaults to None.
        mode (str, optional): The operation mode: "detect" for detection or "repair" for repair.
            Defaults to "detect".
        distributed (bool, optional): If True, uses distributed strategies; otherwise, uses local strategies.
            Defaults to True.
        detect_strategy (str, optional): The detection strategy key to use from the strategy factory.
            Defaults to "regex".
        repair_strategy (str, optional): The repair strategy key to use from the strategy factory.
            Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, applicable to
            threshold-based strategies. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): The type of threshold (e.g., count or proportion)
            for threshold-based strategies. Defaults to None.
        unit (Literal["word", "character"], optional): The unit for proportion thresholds (e.g., words or characters).
            Applicable when `threshold_type` is "proportion". Defaults to None.
        **kwargs: Additional keyword arguments for configuring strategies or customization.

    Methods:
        Inherits methods from `Anomaly`, providing functionality for detecting and repairing
        text anomalies using the specified strategies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        pattern: str = None,
        replacement: str = None,
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "regex",
        repair_strategy: str = "regex_replace",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:
        new_column = new_column
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory=(
                DistributedTextStrategyFactory
                if distributed
                else LocalTextStrategyFactory
            ),
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class NumericAnomaly(Anomaly):
    """
    A class for detecting and repairing text anomalies using various strategies.

    This class allows users to handle text anomalies such as noise, encoding issues, or
    invalid characters using customizable detection and repair strategies. It supports both
    distributed and local execution modes and provides flexibility to configure thresholds,
    patterns, and repair logic.

    Args:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection or repair results.
            Automatically prefixed for consistency.
        mode (str, optional): The operation mode: "detect" for detection or "repair" for repair.
            Defaults to "detect".
        distributed (bool, optional): If True, uses distributed strategies; otherwise, uses local strategies.
            Defaults to True.
        detect_strategy (str): The detection strategy key to use from the strategy factory.
        repair_strategy (str): The repair strategy key to use from the strategy factory.
        threshold (Union[float, int], optional): The threshold value for anomaly detection, applicable to
            threshold-based strategies. Defaults to None.
        **kwargs: Additional keyword arguments for configuring strategies or customization.

    Methods:
        Inherits methods from `Anomaly`, providing functionality for detecting and repairing
        text anomalies using the specified strategies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory: Type[NumericStrategyFactory] = NumericStrategyFactory,
        mode: str = "detect",
        distributed: bool = True,
        threshold: Union[float, int] = None,
        detect_less_than_threshold: bool = None,
        **kwargs,
    ) -> None:
        new_column = new_column
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory=strategy_factory,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class CategoricalAnomaly(Anomaly):
    """
    A base class for detecting or repairing anomalies in categorical data.

    This class provides functionality to handle categorical anomalies by validating
    values against a predefined list of valid categories. It supports both detection
    and repair modes, with configurable strategies for each.

    Attributes:
        column (str): The name of the column to evaluate for anomalies.
        new_column (str): The name of the column to store detection or repair results.
            This column is automatically prefixed for consistency.
        detect_strategy (str): The key for the detection strategy to use.
        repair_strategy (str): The key for the repair strategy to use.
        strategy_factory (Type[CategoricalStrategyFactory]): The factory class to use
            for retrieving detection and repair strategies. Defaults to `CategoricalStrategyFactory`.
        mode (str): The operation mode: "detect" for anomaly detection or "repair" for anomaly repair.
        distributed (bool): If True, uses distributed strategies; otherwise, uses local strategies.
        valid_categories (list): A list of valid categorical values to compare against.

    Methods:
        Inherits methods from `Anomaly`, providing functionality for detecting and repairing
        categorical anomalies.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory: Type[CategoricalStrategyFactory] = CategoricalStrategyFactory,
        mode: str = "detect",
        distributed: bool = True,
        valid_categories: list = None,
        **kwargs,
    ) -> None:
        if not valid_categories:
            raise TypeError(
                "The valid_categories argument must be a list of strings or numbers."
            )
        new_column = new_column
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory=strategy_factory,
            valid_categories=valid_categories,
            **kwargs,
        )
