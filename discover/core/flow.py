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
# Modified   : Monday November 11th 2024 03:10:17 am                                               #
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

    RAW = ("raw", "00_raw", "Raw Dataset")
    INGEST = ("ingest", "01_ingest", "Data Ingestion Stage")
    SENTIMENT = ("sentiment", "02_sentiment", "Sentiment Classification Stage")
    TQA = ("tqa", "03_tqa", "Text Quality Stage")
    QUANT = ("quant", "04_quant", "Quantitative Enrichment Stage")
    AGG = ("agg", "05_agg", "Aggregation Stage")
    DQA = ("dqa", "02_dqa", "Data Quality Assessment")
    CLEAN = ("clean", "03_clean", "Data Cleaning Stage")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class PTModelingStageDef(StageDef):
    """"""

    PRETRAINING = ("pretraining", "00_pretraining", "ABSA Pretraining Stage")
    FINETUNE1 = ("fine_tune_1", "01_fine_tune", "Model Fine-Tuning: Stage 1")
    FINETUNE2 = ("fine_tune_2", "02_fine_tune", "Model Fine-Tuning: Stage 2")
    FINETUNE3 = ("fine_tune_3", "03_fine_tune", "Model Fine-Tuning: Stage 3")
    FINETUNE4 = ("fine_tune_4", "04_fine_tune", "Model Fine-Tuning: Stage 4")
    FINETUNE5 = ("fine_tune_5", "05_fine_tune", "Model Fine-Tuning: Stage 5")
    ABSA = ("absa", "06_absa", "Aspect-Based Sentiment Analysis Stage")
    EMOTION = ("emotion", "07_emotion", "Emotion Analysis Stage")

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
    ENRICHMENT = ("enrichment", "02_enrichment", "Data Enrichment Phase")
    AGGREGATION = ("aggregation", "03_aggregation", "Data Aggregation Phase")

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
