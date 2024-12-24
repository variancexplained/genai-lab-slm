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
# Modified   : Monday December 23rd 2024 04:20:32 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Anomaly Task Base Class Module"""
from discover.core.data_structure import DataFrameStructure
from discover.flow.task.base import Task
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
        self._new_column = f"{self.stage.id}_{new_column}"
        self._detect_strategy = detect_strategy
        self._repair_strategy = repair_strategy
        self._strategy_factory = strategy_factory_cls()
        self._mode_map = {
            "detect": self.detect,
            "repair": self.repair,
        }
        self._kwargs = kwargs

    @task_logger
    def run(self, data: DataFrameStructure) -> DataFrameStructure:
        """
        Executes the specified mode of the anomaly task.

        Args:
            data (DataFrameStructure): The dataset to process.

        Returns:
            DataFrameStructure: The processed dataset after running the specified mode.

        Raises:
            KeyError: If the mode is not supported or improperly mapped.
        """
        return self._mode_map[self._mode](data=data)

    def detect(self, data: DataFrameStructure) -> DataFrameStructure:
        """
        Detects anomalies in the dataset.

        Args:
            data (DataFrameStructure): The dataset to analyze for anomalies.

        Returns:
            DataFrameStructure: The dataset with anomalies flagged in the detection column.

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

    def repair(self, data: DataFrameStructure) -> DataFrameStructure:
        """
        Repairs anomalies in the dataset.

        Args:
            data (DataFrameStructure): The dataset with detected anomalies to repair.

        Returns:
            DataFrameStructure: The dataset with anomalies repaired.

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
