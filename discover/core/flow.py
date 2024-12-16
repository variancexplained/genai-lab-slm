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
# Modified   : Sunday December 15th 2024 03:50:19 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class StageDef(Enum):
    """Base class for all Stage Enums."""

    # Data Prep Stages
    INGEST = ("ingest", "ing", "01_ingest", "Data Ingestion Stage")
    SENTIMENT = ("sentiment", "sa", "02_sentiment", "Sentiment Analysis Stage")
    PERPLEXITY = ("perplexity", "pa", "03_perplexity", "Perplexity Analysis Stage")
    DQD = ("dqd", "dp", "04_dqd", "Data Quality Anomaly Detection Stage")
    CLEAN = ("clean", "dp", "05_clean", "Data Cleaning Stage")
    DQV = ("dqv", "dp", "06_dqv", "Data Cleaning Verification Stage")
    ENRICH_REVIEW = ("enrich_review", "en", "06_enrich", "Review Enrichment Stage")
    ENRICH_APP = ("enrich_app", "en", "07_enrich", "App Enrichment Stage")
    ENRICH_CATEGORY = (
        "enrich_category",
        "en",
        "08_enrich",
        "Category Enrichment Stage",
    )

    # Feature Engineering
    TQA = ("tqa", "tqa", "01_tqa", "Text Quality Analysis Stage")

    @classmethod
    def from_value(cls, value) -> StageDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, stage_id: str, directory: str, description: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.id = stage_id
        obj.directory = directory
        obj.description = description
        return obj


# ------------------------------------------------------------------------------------------------ #
class PhaseDef(Enum):
    # Defining phases with name, directory, and description
    DATAPREP = ("dataprep", "01_dataprep", "Data Preparation Phase")

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
