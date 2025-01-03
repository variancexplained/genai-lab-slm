#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/quality/relevance.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 04:34:56 pm                                             #
# Modified   : Friday January 3rd 2025 01:01:16 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import List, Literal, Optional, Type, Union

import pandas as pd
import pyspark
import pyspark.sql

from discover.flow.base.task import Task
from discover.flow.dataprep.quality.base import TextAnomalyDetectRepairTask
from discover.flow.dataprep.quality.strategy.text.distributed import (
    TextStrategyFactory as SparkTextStrategyFactory,
)
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairNonEnglishTask(TextAnomalyDetectRepairTask):
    """
    Class for detecting or repairing non-English text anomalies.

    This class extends the `TextAnomalyDetectRepairTask` class and provides functionality for identifying
    and repairing non-English content in text columns. It supports detection and repair
    strategies for non-English content, with customizable thresholds for anomaly detection.

    Args:
        column (str, optional): The name of the column in the dataset to apply anomaly detection. Defaults to "content".
        new_column (str, optional): The name of the new column that will store the results of the non-English detection/repair. Defaults to "contains_non_english".
        replacement (str, optional): A string to replace detected non-English content. Defaults to " ".
        mode (str, optional): The mode of operation, either "detect" or "repair". Defaults to "detect".
        strategy_factory_cls (str, optional): The ID of the strategy factory to use. Defaults to "text_spark".
        detect_strategy (str, optional): The strategy to use for detecting non-English content. Defaults to "non_english".
        repair_strategy (str, optional): The strategy to use for repairing detected non-English content. Defaults to "non_english".
        threshold (Union[float, int], optional): The threshold value for anomaly detection, either as a count or proportion. Defaults to None.
        threshold_type (Literal["count", "proportion"], optional): Specifies if the threshold is based on a count or proportion. Defaults to None.
        unit (Literal["word", "character"], optional): Specifies whether to apply the threshold to words or characters. Defaults to None.

    Attributes:
        column (str): The name of the column in the dataset to apply anomaly detection.
        new_column (str): The name of the new column that will store the results of the non-English detection/repair.
        replacement (str): A string to replace detected non-English content.
        mode (str): The mode of operation, either "detect" or "repair".
        strategy_factory_cls (str): The ID of the strategy factory to use.
        detect_strategy (str): The strategy to use for detecting non-English content.
        repair_strategy (str): The strategy to use for repairing detected non-English content.
        threshold (Union[float, int]): The threshold value for anomaly detection, either as a count or proportion.
        threshold_type (str): Specifies if the threshold is based on a count or proportion.
        unit (str): Specifies whether to apply the threshold to words or characters.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "contains_non_english",
        replacement: str = " ",
        mode: str = "detect",
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DetectOrRepairShortReviewsTask(TextAnomalyDetectRepairTask):
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
        Inherits methods from `NumericAnomalyDetectRepairTask`, including functionality for detecting and
        repairing numeric anomalies related to short reviews.
    """

    def __init__(
        self,
        column: str = "content",
        new_column: str = "short_review",
        mode: str = "detect",
        dftype: str = "spark",
        threshold: float = 4,
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
        detect_less_than_threshold: bool = True,
        detect_strategy: str = "short_review",
        repair_strategy: str = "short_review",
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
class DropColumnsTask(Task):
    """
    Class to drops irrelevant specified columns or columns with specified prefixes from a DataFrame.

    This class works for both PySpark and Pandas DataFrames. It removes columns by
    exact names or by checking if the column names start with any of the given prefixes.

    Args:
        columns (Optional[List[str]]): A list of column names to be dropped. Default is an empty list.
        prefix (Optional[List[str]]): A list of prefixes. Columns whose names start with any of these prefixes will be dropped. Default is an empty list.

    Methods:
        run(data): Removes specified columns or columns with specified prefixes from the input DataFrame.
    """

    def __init__(
        self,
        columns: Optional[List[str]] = None,
        prefix: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._columns = columns or []
        self._prefix = prefix or []

    @task_logger
    def run(
        self, data: Union[pyspark.sql.DataFrame, pd.DataFrame]
    ) -> Union[pyspark.sql.DataFrame, pd.DataFrame]:
        """
        Drops specified columns or columns with specified prefixes from the input DataFrame.

        This method identifies columns to drop by exact name and by prefix, then drops them
        from the input DataFrame, which can either be a PySpark or Pandas DataFrame.

        Args:
            data (Union[pyspark.sql.DataFrame, pd.DataFrame]):
                The input DataFrame (either PySpark or Pandas).

        Returns:
            Union[pyspark.sql.DataFrame, pd.DataFrame]:
                A new DataFrame with the specified columns removed.

        Raises:
            TypeError:
                If the input data is neither a Pandas nor a PySpark DataFrame.

        """
        # Identify columns to drop by name
        columns_to_drop = set(self._columns)

        # Identify columns to drop by prefix
        if self._prefix:
            prefix_columns = [
                col
                for col in data.columns
                if any(col.startswith(p) for p in self._prefix)
            ]
            columns_to_drop.update(prefix_columns)

        try:
            # Drop columns for PySpark DataFrame
            return data.drop(*columns_to_drop)
        except AttributeError:
            # Drop columns for Pandas DataFrame (when PySpark's drop method is not available)
            return data.drop(columns=columns_to_drop, axis=1)
        except Exception as e:
            raise TypeError("Input must be a Pandas or PySpark DataFrame") from e
