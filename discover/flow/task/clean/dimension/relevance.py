#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/dimension/relevance.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:34:56 pm                                             #
# Modified   : Sunday November 24th 2024 05:29:41 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Literal, Union

from discover.flow.task.clean.dimension.anomaly import NumericAnomaly, TextAnomaly


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonEnglishTask(TextAnomaly):
    """
    A PySpark task class for detecting or removing non-English text in a DataFrame column.
    The class provides functionality to either flag non-English text using FastText and Lingua
    language detection libraries or remove rows where the text is non-English.

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

    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_non_english",
        replacement: str = " ",
        mode: str = "detect",
        distributed: bool = True,
        detect_strategy: str = "non_english",
        repair_strategy: str = "non_english",
        threshold: Union[float, int] = None,
        threshold_type: Literal["count", "proportion"] = None,
        unit: Literal["word", "character"] = None,
        **kwargs,
    ) -> None:

        super().__init__(
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
class DetectOrRepairShortReviewsTask(NumericAnomaly):
    """
    A task for detecting or repairing short reviews based on numeric thresholds.

    This task evaluates numeric data, such as perplexity or length, to identify
    reviews that are too short. Users can configure thresholds, operation modes
    ("detect" or "repair"), and whether to use distributed or local execution.

    Args:
        column (str, optional): The name of the column to evaluate for short reviews.
            Defaults to "pa_perplexity".
        new_column (str, optional): The name of the column to store detection or repair results.
            Defaults to "contains_gibberish".
        mode (str, optional): The operation mode: "detect" for anomaly detection or "repair"
            for anomaly repair. Defaults to "detect".
        distributed (bool, optional): If True, uses distributed strategies; otherwise, uses local strategies.
            Defaults to True.
        threshold (float, optional): The numeric threshold for identifying short reviews.
            Defaults to 4.
        detect_less_than_threshold (bool, optional): If True, detects values less than the threshold
            as anomalies. If False, detects values greater than the threshold as anomalies.
            Defaults to True.
        detect_strategy (Type[ThresholdAnomalyDetectStrategy], optional): The detection strategy class
            to use for identifying anomalies. Defaults to `ThresholdAnomalyDetectStrategy`.
        repair_strategy (Type[ThresholdAnomalyRepairStrategy], optional): The repair strategy class
            to use for handling anomalies. Defaults to `ThresholdAnomalyRepairStrategy`.
        **kwargs: Additional keyword arguments for advanced configuration or strategy customization.

    Methods:
        Inherits methods from `NumericAnomaly`, including functionality for detecting and
        repairing numeric anomalies related to short reviews.
    """

    def __init__(
        self,
        column: str = "review_length",
        new_column: str = "short_review",
        mode: str = "detect",
        distributed: bool = True,
        threshold: float = 4,
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
