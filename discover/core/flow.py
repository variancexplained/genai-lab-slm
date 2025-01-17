#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/flow.py                                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 08:36:22 pm                                            #
# Modified   : Thursday January 16th 2025 07:14:48 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Defines all workflow stages."""

    # Data prep phase
    RAW = ("raw", 0, "Raw Data Stage")
    INGEST = ("ingest", 1, "Data Ingestion Stage")
    DQA = ("dqa", 2, "Data Quality Assessment Stage")
    SEMICLEAN = ("semiclean", 3, "Semi-Clean Data Stage")
    DQV = ("dqv", 4, "Data Quality Verification Stage")
    CLEAN = ("clean", 5, "Clean Data Stage")
    # Enrichment Phase
    TQA = ("tqa", 0, "Text Quality Analysis")
    SENTIMENT = ("sentiment", 1, "Sentiment Classification Stage")
    ASPECT = ("aspect", 2, "Aspect Extraction Stage")
    TOPIC = ("topic", 3, "Topic Analysis Stage")
    APP = ("app", 4, "App Aggregation Stage")
    CATEGORY = ("category", 5, "Category Aggregation Stage")
    STATS = ("stats", 6, "Statistical Features Stage ")
    SEGMENTATION = ("segmentation", 7, "User Segmentation Stage")

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
    SMOKE_TEST = ("smoke", 4, "Smoke Testing")

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
    ACQUISITION = ("acquisition", 0, "Data Acquisition Phase")
    DATAPREP = ("dataprep", 1, "Data Preparation Phase")
    ENRICHMENT = ("enrichment", 2, "Data Enrichment Phase")
    EDA = ("eda", 3, "Exploratory Data Analysis")
    ABSA_FT = ("absa_ft", 4, "ABSA Model Fine-Tuning Phase")
    ABSA_CD = ("absa_cd", 5, "ABSA Custom Model Development Phase")
    TESTING = ("test", 6, "Testing Phase")

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


# ------------------------------------------------------------------------------------------------ #
class FlowStateDef(Enum):
    CREATED = "created"
    PENDING = "pending"
    STARTED = "started"
    EXCEPTION = "exception"
    COMPLETED = "complete"
