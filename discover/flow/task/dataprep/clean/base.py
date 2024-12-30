#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/dataprep/clean/base.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:27:43 am                                             #
# Modified   : Monday December 30th 2024 03:13:30 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Prep Cleaning Task Base Module"""
from typing import Literal, Type, Union

from discover.asset.dataset import DFType
from discover.flow.task.base import Task
from discover.flow.task.dataprep.clean.strategy.categorical import (
    CategoricalStrategyFactory,
)
from discover.flow.task.dataprep.clean.strategy.discrete import DiscreteStrategyFactory
from discover.flow.task.dataprep.clean.strategy.interval import IntervalStrategyFactory
from discover.flow.task.dataprep.clean.strategy.nominal import NominalStrategyFactory
from discover.flow.task.dataprep.clean.strategy.numeric import NumericStrategyFactory
from discover.flow.task.dataprep.clean.strategy.text.distributed import (
    TextStrategyFactory as SparkTextStrategyFactory,
)
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
#                                      ANOMALY                                                     #
# ------------------------------------------------------------------------------------------------ #
class AnomalyDetectRepairTask(Task):
    """
    Base class for handling anomalies in data.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection or repair results.
        mode (str): The operation mode ("detect" or "repair").
        detect_strategy (str): The name of the detection strategy to use.
        repair_strategy (str): The name of the repair strategy to use.
        strategy_factory_cls (str): The id for the StrategyFactory from which Strategies are
            provided.
        **kwargs: Additional arguments for specific anomaly configurations.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: str,
        **kwargs,
    ) -> None:

        super().__init__(phase=kwargs["phase"], stage=kwargs["stage"])
        self._column = column
        self._mode = mode
        self._new_column = new_column
        self._detect_strategy = detect_strategy
        self._repair_strategy = repair_strategy
        self._strategy_factory = strategy_factory_cls()
        self._mode_map = {
            "detect": self.detect,
            "repair": self.repair,
        }
        self._kwargs = kwargs

    @task_logger
    def run(self, data: DFType) -> DFType:
        """
        Executes the specified mode of the anomaly task.

        Args:
            data (DFType): The dataset to process.

        Returns:
            DFType: The processed dataset after running the specified mode.

        Raises:
            KeyError: If the mode is not supported or improperly mapped.
        """
        return self._mode_map[self._mode](data=data)

    def detect(self, data: DFType) -> DFType:
        """
        Detects anomalies in the dataset.

        Args:
            data (DFType): The dataset to analyze for anomalies.

        Returns:
            DFType: The dataset with anomalies flagged in the detection column.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        strategy_cls = self._strategy_factory.get_detect_strategy(
            strategy_type=self._detect_strategy
        )
        strategy = strategy_cls(
            column=self._column, new_column=self._new_column, **self._kwargs
        )
        return strategy.detect(data=data)

    def repair(self, data: DFType) -> DFType:
        """
        Repairs anomalies in the dataset.

        Args:
            data (DFType): The dataset with detected anomalies to repair.

        Returns:
            DFType: The dataset with anomalies repaired.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        strategy_cls = self._strategy_factory.get_repair_strategy(
            strategy_type=self._repair_strategy
        )
        strategy = strategy_cls(
            column=self._column,
            new_column=self._new_column,
            **self._kwargs,
        )
        return strategy.repair(data=data)


# ------------------------------------------------------------------------------------------------ #
class TextAnomalyDetectRepairTask(AnomalyDetectRepairTask):
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
        strategy_factory_cls (Type[SparkTextStrategyFactory]): SparkTextStrategyFactory subclass responsible for providing the
            factory for detect and repair strategies.

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
        strategy_factory_cls (Type[SparkTextStrategyFactory]): SparkTextStrategyFactory subclass responsible for providing the
            factory for detect and repair strategies.
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
        strategy_factory_cls: Type[SparkTextStrategyFactory] = SparkTextStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            threshold=threshold,
            threshold_type=threshold_type,
            unit=unit,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class NumericAnomalyDetectRepairTask(AnomalyDetectRepairTask):
    """
    Handles the detection and repair of numerical anomalies.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column where the results will be stored.
        detect_strategy (str): The strategy to use for detecting anomalies.
        repair_strategy (str): The strategy to use for repairing anomalies.
        strategy_factory_cls (Type[NumericStrategyFactory], optional): The factory class to create the strategy. Defaults to NumericStrategyFactory.
        mode (str, optional): The mode for the task, either 'detect' or 'repair'. Defaults to 'detect'.
        threshold (Union[float, int], optional): The threshold value for anomaly detection.
        detect_less_than_threshold (bool, optional): If True, detects values less than the threshold. Defaults to None.
        **kwargs: Additional keyword arguments to be passed to the parent class.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: Type[NumericStrategyFactory] = NumericStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            mode=mode,
            threshold=threshold,
            detect_less_than_threshold=detect_less_than_threshold,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class CategoricalAnomalyDetectRepairTask(AnomalyDetectRepairTask):
    """
    Handles the detection and repair of anomalies in categorical data columns.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column where the results will be stored.
        detect_strategy (str): The strategy to use for detecting anomalies.
        repair_strategy (str): The strategy to use for repairing anomalies.
        strategy_factory_cls (Type[CategoricalStrategyFactory], optional): The factory class to create the strategy. Defaults to CategoricalStrategyFactory.
        mode (str, optional): The mode for the task, either 'detect' or 'repair'. Defaults to 'detect'.
        valid_categories (list): A list of valid categories for the categorical data.
        **kwargs: Additional keyword arguments to be passed to the parent class.

    Raises:
        TypeError: If the `valid_categories` argument is not provided or is not a list.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: Type[
            CategoricalStrategyFactory
        ] = CategoricalStrategyFactory,
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
            strategy_factory_cls=strategy_factory_cls,
            mode=mode,
            valid_categories=valid_categories,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class NominalAnomalyDetectRepairTask(AnomalyDetectRepairTask):
    """
    Handles the detection and repair of anomalies in nominal (categorical) data columns.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column where the results will be stored.
        detect_strategy (str): The strategy to use for detecting anomalies.
        repair_strategy (str): The strategy to use for repairing anomalies.
        strategy_factory_cls (Type[NominalStrategyFactory], optional): The factory class to create the strategy. Defaults to NominalStrategyFactory.
        mode (str, optional): The mode for the task, either 'detect' or 'repair'. Defaults to 'detect'.
        **kwargs: Additional keyword arguments to be passed to the parent class.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: Type[NominalStrategyFactory] = NominalStrategyFactory,
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_cls=strategy_factory_cls,
            mode=mode,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class IntervalAnomaly(AnomalyDetectRepairTask):
    """
    Handles the detection and repair of anomalies in interval data columns.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column where the results will be stored.
        detect_strategy (str): The strategy to use for detecting anomalies.
        repair_strategy (str): The strategy to use for repairing anomalies.
        strategy_factory_cls (Type[IntervalStrategyFactory], optional): The factory class to create the strategy. Defaults to IntervalStrategyFactory.
        mode (str, optional): The mode for the task, either 'detect' or 'repair'. Defaults to 'detect'.
        **kwargs: Additional keyword arguments to be passed to the parent class.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: Type[IntervalStrategyFactory] = IntervalStrategyFactory,
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_cls=strategy_factory_cls,
            mode=mode,
            **kwargs,
        )


# ------------------------------------------------------------------------------------------------ #
class DiscreteAnomaly(AnomalyDetectRepairTask):
    """
    Handles the detection and repair of anomalies in discrete data columns.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column where the results will be stored.
        detect_strategy (str): The strategy to use for detecting anomalies.
        repair_strategy (str): The strategy to use for repairing anomalies.
        strategy_factory_cls (Type[DiscreteStrategyFactory], optional): The factory class to create the strategy. Defaults to DiscreteStrategyFactory.
        mode (str, optional): The mode for the task, either 'detect' or 'repair'. Defaults to 'detect'.
        **kwargs: Additional keyword arguments to be passed to the parent class.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory_cls: Type[DiscreteStrategyFactory] = DiscreteStrategyFactory,
        mode: str = "detect",
        **kwargs,
    ) -> None:
        super().__init__(
            column=column,
            new_column=new_column,
            mode=mode,
            detect_strategy=detect_strategy,
            repair_strategy=repair_strategy,
            strategy_factory_cls=strategy_factory_cls,
            **kwargs,
        )
