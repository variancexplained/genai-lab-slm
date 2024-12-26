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
# Modified   : Wednesday December 25th 2024 08:54:14 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class StageEnum(Enum):
    """Base class describing stage enums."""


# ------------------------------------------------------------------------------------------------ #
class DataPrepStageEnum(StageEnum):

    RAW = ("raw", "00_raw", "Raw Data Stage")
    INGEST = ("ingest", "01_ingest", "Data Ingestion Stage")
    DQD = ("dqd", "02_dqd", "Data Quality Anomaly Detection Stage")
    SEMICLEAN = ("semiclean", "03_semiclean", "Semi-Clean Data Stage")
    DQV = ("dqv", "04_dqv", "Data Quality Verification Stage")
    CLEAN = ("clean", "05_clean", "Clean Data Stage")

    @classmethod
    def from_value(cls, value) -> DataPrepStageEnum:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name

        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class DataEnrichmentStageEnum(StageEnum):

    SENTIMENT = ("sentiment", "00_sentiment", "Sentiment Classification Stage")
    QUANT = (
        "quantitative",
        "01_quantitative",
        "Quantitative Data Enrichment Stage",
    )
    APP = ("app", "02_app", "App Enrichment Stage")
    CATEGORY = (
        "category",
        "03_categpru",
        "Category Enrichment Stage",
    )

    @classmethod
    def from_value(cls, value) -> DataEnrichmentStageEnum:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class ModelStageEnum(StageEnum):

    FINETUNING = ("fine_tuning", "00_fine_tuning", "Model Fine-Tuning Stage")
    DEVELOPMENT = ("development", "01_development", "Model Development Stage")

    @classmethod
    def from_value(cls, value) -> ModelStageEnum:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class PhaseEnum(Enum):
    # Defining phases with name, directory, and description
    DATAPREP = ("dataprep", "00_dataprep", "Data Preparation Phase")
    ENRICHMENT = ("enrichment", "01_enrichment", "Data Enrichment Phase")
    MODEL = ("model", "02_model", "Modeling Phase")

    def __new__(cls, name: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.directory = directory
        obj.description = description
        return obj

    @classmethod
    def from_value(cls, value) -> PhaseEnum:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")
