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
# Modified   : Friday October 11th 2024 10:55:54 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    # Defining phases with name, directory, and description
    DATAPREP = ("dataprep", "01_dataprep", "Data Preparation Phase")
    FEATURE = (
        "feature",
        "02_feature",
        "Feature Engineering Phase",
    )
    SENTIMENT = ("sentiment", "03_sentiment", "Sentiment Analysis Phase")
    ABSEIA = (
        "abseia",
        "04_abseia",
        "Aspect-Based Sentiment Emotion and Intensity Analysis Phase",
    )
    GENAI = ("genai", "05_genai", "Generative AI Phase")

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

    NORM = ("normalize", "01_normalize", "Data Normalization Stage")
    DQA = ("dqa", "02_dqa", "Data Quality Assessment Stage")
    CLEAN = ("clean", "03_clean", "Data Cleaning Stage")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class EDAStageDef(StageDef):
    """
    EDAStageDef is an enumeration that defines different stages of Exploratory Data Analysis (EDA).

    Attributes:
    -----------
    UNIVARIATE : tuple
        Represents the univariate analysis stage in EDA.
        - value: "00_univariate"
        - description: "EDA - Univariate Analysis"

    BIVARIATE : tuple
        Represents the bivariate analysis stage in EDA.
        - value: "01_bivariate"
        - description: "EDA - Bivariate Analysis"

    MULTIVARIATE : tuple
        Represents the multivariate analysis stage in EDA.
        - value: "02_multivariate"
        - description: "EDA - Multivariate Analysis"

    Methods:
    --------
    __new__(cls, value: str, description: str) -> EDAStageDef:
        Creates a new instance of EDAStageDef with the specified value and description.

    Parameters:
    -----------
    value : str
        The internal value representing the EDA stage.

    description : str
        A human-readable description of the EDA stage.
    """

    UNIVARIATE = ("00_univariate", "EDA - Univariate Analysis")
    BIVARIATE = ("01_bivariate", "EDA - Bivariate Analysis")
    MULTIVARIATE = ("02_multivariate", "EDA - Multivariate Analysis")

    def __new__(cls, value: str, description: str) -> EDAStageDef:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class ModelingStageDef(StageDef):
    """Modeling stage definition"""


# ------------------------------------------------------------------------------------------------ #
class SentimentStageDef(StageDef):
    """Sentiment Analysis stage definition"""


# ------------------------------------------------------------------------------------------------ #
class OpportunityStageDef(StageDef):
    """Sentiment Analysis stage definition"""
