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
# Modified   : Sunday December 15th 2024 05:55:57 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Union

from discover.flow.task.clean.base.anomaly import Anomaly


# ------------------------------------------------------------------------------------------------ #
class TextAnomaly(Anomaly):
    """
    Class for handling text anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in text data. It supports pattern-based detection and
    replacement strategies, and also provides options for threshold-based anomaly
    detection using different units (words or characters).

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        pattern (str, optional): A regex pattern for detecting anomalies in the text data. Defaults to None.
        replacement (str, optional): A string to replace detected anomalies in the text. Defaults to None.
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        distributed (bool, optional): Whether the anomaly detection should be done in a distributed manner. Defaults to True.
        detect_strategy (str, optional): The strategy to use for detecting anomalies in the text. Defaults to "regex".
        repair_strategy (str, optional): The strategy to use for repairing detected anomalies. Defaults to "regex_replace".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "text_spark".

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        pattern (str): A regex pattern for detecting anomalies in the text data.
        replacement (str): A string to replace detected anomalies in the text.
        mode (str): The mode of operation, either "detect" or "repair".
        distributed (bool): Whether the anomaly detection should be done in a distributed manner.
        detect_strategy (str): The strategy to use for detecting anomalies in the text.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
        strategy_factory_id (str): The ID of the strategy factory to use.
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
        strategy_factory_id: str = "text_spark",
        **kwargs,
    ) -> None:
        super().__init__(
            pattern=pattern,
            column=column,
            new_column=new_column,
            replacement=replacement,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class NumericAnomaly(Anomaly):
    """
    Class for handling numeric anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in numeric data. It utilizes specific detection and repair
    strategies, defined via the provided strategies and factory class. Additionally, it
    supports configuring thresholds for anomaly detection and adjusting the detection mode.

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "numeric".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        threshold (Union[float, int], optional): The threshold value used for detecting anomalies. If not provided, no threshold is applied.
        detect_less_than_threshold (bool, optional): If True, anomalies are detected for values less than the threshold. Defaults to None.

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str): The ID of the strategy factory to use.
        mode (str): The mode of operation, either "detect" or "repair".
        threshold (Union[float, int]): The threshold value used for detecting anomalies.
        detect_less_than_threshold (bool): If True, anomalies are detected for values less than the threshold.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_id: str = "numeric",
        mode: str = "detect",
        threshold: Union[float, int] = None,
        detect_less_than_threshold: bool = None,
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            mode=mode,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class CategoricalAnomaly(Anomaly):
    """
    Class for handling categorical anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in categorical data. It utilizes specific detection and repair
    strategies, defined via the provided strategies and factory class. Additionally, it
    ensures that only valid categories are processed.

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "categorical".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        valid_categories (list, optional): A list of valid categories that should be present in the data.
            If not provided, a `TypeError` is raised.

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str): The ID of the strategy factory to use.
        mode (str): The mode of operation, either "detect" or "repair".
        valid_categories (list): The list of valid categories to be considered for anomaly detection.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_id: str = "categorical",
        mode: str = "detect",
        valid_categories: list = None,
        **kwargs,
    ) -> None:
        if not valid_categories:
            raise TypeError("The valid_categories argument must be a list.")

        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            mode=mode,
            valid_categories=valid_categories,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class NominalAnomaly(Anomaly):
    """
    Class for handling nominal anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in nominal data. It utilizes specific detection and repair
    strategies, defined via the provided strategies and factory class.

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "nominal".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str): The ID of the strategy factory to use.
        mode (str): The mode of operation, either "detect" or "repair".
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_id: str = "nominal",
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            mode=mode,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class IntervalAnomaly(Anomaly):
    """
    Class for handling interval anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in interval data. It utilizes specific detection and repair
    strategies, defined via the provided strategies and factory class.

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "interval".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str): The ID of the strategy factory to use.
        mode (str): The mode of operation, either "detect" or "repair".
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_id: str = "interval",
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            mode=mode,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DiscreteAnomaly(Anomaly):
    """
    Class for handling discrete anomaly detection and repair strategies.

    This class extends the `Anomaly` class and provides functionality for detecting
    and repairing anomalies in discrete data. It utilizes specific detection and repair
    strategies, defined via the provided strategies and factory class.

    Args:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str, optional): The ID of the strategy factory to use. Defaults to "discrete".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        **kwargs: Additional arguments passed to the parent class `Anomaly`.

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store repaired values.
        detect_strategy (str): The strategy to use for detecting anomalies in the column.
        repair_strategy (str): The strategy to use for repairing detected anomalies.
        strategy_factory_id (str): The ID of the strategy factory to use.
        mode (str): The mode of operation, either "detect" or "repair".
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_id: str = "discrete",
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_id=strategy_factory_id,
            **kwargs,
        )
