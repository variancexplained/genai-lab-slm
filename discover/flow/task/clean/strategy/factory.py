#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/strategy/factory.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 15th 2024 04:54:31 am                                               #
# Modified   : Sunday December 15th 2024 05:43:20 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Cleaning Strategy Factory Module"""
from __future__ import annotations

from enum import Enum

from discover.flow.task.clean.base.factory import StrategyFactory
from discover.flow.task.clean.strategy.categorical import CategoricalStrategyFactory
from discover.flow.task.clean.strategy.discrete import DiscreteStrategyFactory
from discover.flow.task.clean.strategy.interval import IntervalStrategyFactory
from discover.flow.task.clean.strategy.nominal import NominalStrategyFactory
from discover.flow.task.clean.strategy.numeric import NumericStrategyFactory
from discover.flow.task.clean.strategy.text.distributed import (
    TextStrategyFactory as SparkTextStrategyFactory,
)
from discover.flow.task.clean.strategy.text.local import TextStrategyFactory


# ------------------------------------------------------------------------------------------------ #
class CleanStrategyFactory(Enum):
    """
    Enum for mapping cleaning strategies to their respective factory classes.

    This enum provides a central registry for different data cleaning strategies,
    associating each strategy type with its corresponding factory class. These factory
    classes are responsible for creating instances of cleaning strategies tailored to
    specific data types or processing requirements.

    Members:
        TEXT: Factory for handling text data cleaning strategies.
        TEXT_SPARK: Factory for handling Spark-based text data cleaning strategies.
        CATEGORICAL: Factory for handling categorical data cleaning strategies.
        DISCRETE: Factory for handling discrete data cleaning strategies.
        INTERVAL: Factory for handling interval data cleaning strategies.
        NOMINAL: Factory for handling nominal data cleaning strategies.
        NUMERIC: Factory for handling numeric data cleaning strategies.

    Usage:
        To retrieve a factory class for a specific cleaning strategy, access the enum
        member and use its `.strategy_factory_id` property. For example:

        >>> factory_class = CleanStrategyFactory.TEXT.strategy_factory_id
        >>> factory_instance = factory_class()

        Alternatively, you can use the `from_factory` method to get the factory class
        based on its identifier:

        >>> factory_class = CleanStrategyFactory.from_factory('text')
        >>> factory_instance = factory_class()
    """

    TEXT = ("text", TextStrategyFactory)
    TEXT_SPARK = ("text_spark", SparkTextStrategyFactory)
    CATEGORICAL = ("categorical", CategoricalStrategyFactory)
    DISCRETE = ("discrete", DiscreteStrategyFactory)
    INTERVAL = ("interval", IntervalStrategyFactory)
    NOMINAL = ("nominal", NominalStrategyFactory)
    NUMERIC = ("numeric", NumericStrategyFactory)

    def __new__(
        cls, value: str, strategy_factory_id: StrategyFactory
    ) -> CleanStrategyFactory:
        obj = object.__new__(cls)
        obj._value_ = value.lower()  # Ensure value is case-insensitive
        obj._factory = strategy_factory_id
        return obj

    @classmethod
    def from_factory(cls, factory: str) -> StrategyFactory:
        """
        Retrieve the factory class corresponding to a given string identifier.

        Args:
            factory (str): The identifier for the cleaning strategy (e.g., 'text').

        Returns:
            StrategyFactory: The factory class corresponding to the identifier.

        Raises:
            ValueError: If no matching factory is found for the provided identifier.
        """
        for member in cls:
            if member._value_ == factory.lower():  # Case-insensitive comparison
                return member.strategy_factory_id
        raise ValueError(f"No matching {cls.__name__} for identifier: {factory}")

    @property
    def strategy_factory_id(self) -> StrategyFactory:
        """
        The factory class associated with this cleaning strategy.

        Returns:
            StrategyFactory: The factory class for creating instances of the
            cleaning strategy.
        """
        return self._factory
