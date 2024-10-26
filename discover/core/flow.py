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
# Modified   : Saturday October 26th 2024 09:02:14 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Base class for all Stage Enums."""

    @classmethod
    def from_value(cls, value) -> StageDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")


# ------------------------------------------------------------------------------------------------ #
class DataPrepStageDef(StageDef):
    """"""

    INGEST = ("ingest", "00_ingest", "Data Ingestion Stage")
    CLEAN = ("clean", "01_clean", "Data Cleaning Stage")
    NLP = ("nlp", "02_nlp", "Text Preprocessing Stage")
    FEATURE = ("feature", "03_feature", "Feature Engineering Stage")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class FeatureEngineeringStageDef(StageDef):
    """"""

    REVIEW = ("review", "00_review", "Review Feature Engineering Stage")
    APP = ("app", "01_app", "App Feature Engineering Stage")
    USER = ("user", "02_user", "User Feature Engineering Stage")
    CATEGORY = ("category", "03_category", "Category Feature Engineering Stage")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class AnalysisStageDef(StageDef):
    """"""

    SENTIMENT = ("sentiment", "00_sentiment", "Sentiment Analysis Stage")
    ASPECT = ("aspect", "01_aspect", "Aspect-Based Sentiment Analysis Stage")
    EMOTION = ("emotion", "02_emotion", "Emotion-Intensity Analysis Stage")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    # Defining phases with name, directory, and description
    DATAPREP = ("dataprep", "01_dataprep", "Data Preparation Phase")
    FEATURE = (
        "feature",
        "02_feature",
        "Feature Engineering Phase",
    )
    ANALYSIS = ("analysis", "03_analysis", "Analysis Phase")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj

    @classmethod
    def from_value(cls, value) -> PhaseDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")
