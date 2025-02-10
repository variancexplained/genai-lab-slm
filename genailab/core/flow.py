#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/core/flow.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 08:36:22 pm                                            #
# Modified   : Saturday February 8th 2025 10:43:04 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Defines all workflow stages."""

    # Data prep Stages
    RAW = ("raw", 0, "Raw Data Stage")
    PREPROCESS = ("preprocess", 1, "Data Preprocessing Stage")
    DQA = ("dqa", 2, "Data Quality Assessment Stage")
    SEMICLEAN = ("semiclean", 3, "Semi-Clean Data Stage")
    DQV = ("dqv", 4, "Data Quality Verification Stage")
    CLEAN = ("clean", 5, "Clean Data Stage")
    TQA = ("tqa", 6, "Text Quality Analysis Stage")
    SENTIMENT = ("sentiment", 7, "Sentiment Analysis Stage")
    # Feature Engineering Stage
    INSTANCE = ("instance", 0, "Feature Engineering-Instance Selection Stage")
    # Modeling Stages
    ABSA_FT = ("absa_ft", 0, "Fine-Tuned ABSA Modeling Stage")
    ABSA_CUSTOM = ("absa_custom", 1, "Custom ABSA Modeling Stage")
    # Testing Stages
    TEST = ("test", 10, "Test Stage")

    @classmethod
    def from_value(cls, value) -> StageDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, id: str, label: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.id = id
        obj.label = label
        return obj


# ------------------------------------------------------------------------------------------------ #
class TestStageDef(Enum):
    UNIT_TEST = ("unit", 0, "Unit Testing")
    INTEGRATION_TEST = ("integration", 1, "Integration Testing")
    FUNCTIONAL_TEST = ("functional", 2, "Functional Testing")
    SYSTEM_TEST = ("system", 3, "System Testing")
    PERFORMANCE_TEST = ("performance", 4, "Performance Testing")
    SMOKE_TEST = ("smoke", 5, "Smoke Testing")

    @classmethod
    def from_value(cls, value) -> TestStageDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, id: int, label: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.id = id
        obj.label = label
        return obj


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    DATAPREP = ("dataprep", 0, "Data Preparation Phase")
    FEATURE = ("feature", 1, "Feature Engineering Phase")
    ABSA = ("absa", 2, "ABSA Modeling Phase")
    TEST = ("test", 9, "Testing Phase")

    def __new__(cls, name: str, id: int, label: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.id = id
        obj.label = label
        return obj

    @classmethod
    def from_value(cls, value) -> PhaseDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")
