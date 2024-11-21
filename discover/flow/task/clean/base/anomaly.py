#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/base/anomaly.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 12:27:43 am                                             #
# Modified   : Thursday November 21st 2024 01:29:38 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import abstractmethod

from discover.core.data_structure import DataFrameType
from discover.flow.task.base import Task
from discover.flow.task.clean.base.factory import StrategyFactory
from discover.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
#                                      ANOMALY                                                     #
# ------------------------------------------------------------------------------------------------ #
class Anomaly(Task):
    """
    Base class for handling anomalies in data.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection or repair results.
        mode (str): The operation mode ("detect" or "repair").
        detect_strategy (str): The name of the detection strategy to use.
        repair_strategy (str): The name of the repair strategy to use.
        strategy_factory (StrategyFactory): Factory to retrieve strategies for detection and repair.
        **kwargs: Additional arguments for specific anomaly configurations.

    Properties:
        detection (str): The column where detection results are stored.
    """

    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory: StrategyFactory,
        **kwargs,
    ) -> None:
        super().__init__()
        self._column = column
        self._mode = mode
        self._new_column = new_column
        self._detect_strategy = detect_strategy
        self._repair_strategy = repair_strategy
        self._strategy_factory = strategy_factory()
        self._mode_map = {
            "detect": self.detect,
            "repair": self.repair,
        }

    @property
    def detection(self) -> str:
        """The column where detection results are stored."""
        if self._new_column is None:
            raise ValueError(
                "The `new_column` property must be set to access detection results."
            )
        return self._new_column

    @task_logger
    def run(self, data: DataFrameType) -> DataFrameType:
        """
        Executes the specified mode of the anomaly task.

        Args:
            data (DataFrameType): The dataset to process.

        Returns:
            DataFrameType: The processed dataset after running the specified mode.

        Raises:
            KeyError: If the mode is not supported or improperly mapped.
        """
        return self._mode_map[self._mode](data=data)

    @abstractmethod
    def detect(self, data: DataFrameType) -> DataFrameType:
        """
        Detects anomalies in the dataset.

        Args:
            data (DataFrameType): The dataset to analyze for anomalies.

        Returns:
            DataFrameType: The dataset with anomalies flagged in the detection column.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass

    @abstractmethod
    def repair(self, data: DataFrameType) -> DataFrameType:
        """
        Repairs anomalies in the dataset.

        Args:
            data (DataFrameType): The dataset with detected anomalies to repair.

        Returns:
            DataFrameType: The dataset with anomalies repaired.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
        """
        pass
