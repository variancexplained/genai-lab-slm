#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/substance/value_objects/lifecycle.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:26:24 am                                                    #
# Modified   : Friday September 20th 2024 08:12:47 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class EPhase(Enum):
    """
    Enum representing the different phases of a data processing or analysis pipeline.

    Each phase corresponds to a high-level segment of the workflow, such as data preparation
    or analysis. These phases help to track the current state of the overall pipeline or
    workflow.

    Enum Members:
    -------------
    DATAPREP : str
        Represents the data preparation phase, labeled as "data_prep".
    ANALYSIS : str
        Represents the analysis phase, labeled as "analysis".

    Attributes:
    -----------
    value : str
        The string value associated with each phase (e.g., "data_prep", "analysis").
    description : str
        A human-readable description of the phase (e.g., "Data Preparation Phase", "Analysis Phase").
    """

    DATAPREP = ("00_dataprep", "Data Preparation Phase")
    ANALYSIS = ("01_analysis", "Analysis Phase")
    GENAI = ("02_genai", "Generative AI Phase")

    def __new__(cls, value: str, description: str) -> EPhase:
        """
        Create a new instance of the Phase enum with a string value and a description.

        Parameters:
        -----------
        value : str
            The string identifier for the phase.
        description : str
            A human-readable description of the phase.

        Returns:
        --------
        EPhase
            A new instance of the EPhase enum.
        """
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description  # type: ignore
        return obj


# ------------------------------------------------------------------------------------------------ #
class EStage(Enum):
    """Base class for all Stage Enums."""


# ------------------------------------------------------------------------------------------------ #
class EDataPrepStage(EStage):
    """
    Enum representing the stages of a data processing pipeline in data preparation.

    Each stage corresponds to a specific step in the data pipeline, such as raw data ingestion,
    data quality assessment (DQA), data cleaning, feature engineering, and more.
    These stages are used to track the current phase of data processing within a pipeline or service.

    Enum Members:
    -------------
    RAW : str
        Represents the raw data stage, labeled as "00_raw".
    INGEST : str
        Represents the ingestion stage, labeled as "01_ingest".
    DQA : str
        Represents the data quality assessment stage, labeled as "02_dqa".
    CLEAN : str
        Represents the data cleaning stage, labeled as "03_clean".
    FEATURE : str
        Represents the feature engineering stage, labeled as "04_features".
    TEXTFEATURE : str
        Represents the text feature engineering stage, labeled as "05_text_features".
    AGGTRICS : str
        Represents the aggregation metrics stage, labeled as "06_agg_metrics".
    CORE : str
        Represents core data processing tasks that are used across multiple stages, labeled as "99_core".
    """

    RAW = ("00_raw", "Raw Stage")
    INGEST = ("01_ingest", "Ingestion Stage")
    DQA = ("02_dqa", "Data Quality Assessment Stage")
    CLEAN = ("03_clean", "Data Cleaning Stage")
    FEATURE = ("04_features", "Feature Engineering Stage")
    TEXTFEATURE = ("05_text_features", "Text Feature Engineering Stage")
    AGGTRICS = ("06_agg_metrics", "Aggregation Metrics Stage")
    CORE = (
        "99_core",
        "Non Stage Specific",
    )  # Doesn't belong to a specific stage; used in multiple stages.

    def __new__(cls, value: str, description: str) -> EDataPrepStage:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description  # type: ignore
        return obj


# ------------------------------------------------------------------------------------------------ #
class EAnalysisStage(EStage):
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

    def __new__(cls, value: str, description: str) -> EAnalysisStage:
        obj = object.__new__(cls)
        obj._value_ = value
        obj.description = description  # type: ignore
        return obj


# ------------------------------------------------------------------------------------------------ #
class ModelingStage(EStage):
    """TBD"""
