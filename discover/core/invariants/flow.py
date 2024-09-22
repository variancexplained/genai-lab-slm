#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/invariants/flow.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 21st 2024 08:36:22 pm                                            #
# Modified   : Saturday September 21st 2024 09:25:08 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    """ """

    DATAPREP = ("00_dataprep", "Data Preparation Phase")
    EDA = ("01_eda", "Exploratory Data Analysis Phase")
    SENTIMENT = ("02_sentiment", "Sentiment Analysis Phase")
    DISCOVERY = ("03_discovery", "Opportunity Discovery Phase")

    def __new__(cls, value: str, description: str) -> PhaseDef:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Base class for all Stage Enums."""


# ------------------------------------------------------------------------------------------------ #
class DataPrepStageDef(StageDef):
    """"""

    RAW = ("00_raw", "Raw Stage")
    INGEST = ("01_ingest", "Ingestion Stage")
    DQA = ("02_dqa", "Data Quality Assessment Stage")
    CLEAN = ("03_clean", "Data Cleaning Stage")
    FEATURE = ("04_features", "Feature Engineering Stage")

    def __new__(cls, value: str, description: str) -> DataPrepStageDef:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class AnalysisStageDef(StageDef):
    """
    Enum representing the stages of a data analysis pipeline.

    These stages correspond to steps in the analysis process, such as data ingestion,
    data quality analysis, exploratory data analysis (EDA), and sentiment analysis.

    Enum Members:
    -------------
    INGEST : str
        Represents the data ingestion analysis stage, labeled as "00_ingest".
    DQA : str
        Represents the data quality analysis stage, labeled as "01_dqa".
    EDA : str
        Represents the exploratory data analysis stage, labeled as "O2_eda".
    SENTIMENT : str
        Represents the sentiment and opinion analysis stage, labeled as "03_sentiment".
    """

    INGEST = ("00_ingest", "Data Ingestion Analysis")
    DQA = ("01_dqa", "Data Quality Analysis")
    EDA = ("O2_eda", "Exploratory Data Analysis")
    SENTIMENT = ("03_sentiment", "Sentiment and Opinion Analysis")

    def __new__(cls, value: str, description: str) -> AnalysisStageDef:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description
        return obj
