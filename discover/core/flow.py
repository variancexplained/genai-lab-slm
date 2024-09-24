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
# Modified   : Monday September 23rd 2024 10:54:37 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    # Defining phases with name, directory, and description
    DATAPREP = ("dataprep", "00_dataprep", "Data Preparation Phase")
    TRANSFORMATION = (
        "transformation",
        "01_transformation",
        "Data Transformation Phase",
    )
    MODELING = ("modeling", "02_modeling", "Modeling Phase")
    ANALYSIS = ("analysis", "03_analysis", "Data Analysis Phase")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Base class for all Stage Enums."""


# ------------------------------------------------------------------------------------------------ #
class DataPrepStageDef(StageDef):
    """"""

    RAW = ("raw", "00_raw", "Raw Stage")
    INGEST = ("ingest", "01_ingest", "Ingestion Stage")
    DQA = ("dqa", "02_dqa", "Data Quality Assessment Stage")
    CLEAN = ("clean", "03_clean", "Data Cleaning Stage")
    FEATURE = ("features", "04_features", "Feature Engineering Stage")

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
