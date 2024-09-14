#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/value_objects/lifecycle.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:26:24 am                                                    #
# Modified   : Saturday September 14th 2024 05:52:06 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class Stage(Enum):
    """
    Enum representing the stages of a data processing pipeline.

    Each stage corresponds to a specific step in the data pipeline,
    such as raw data ingestion, data quality assessment (DQA), data cleaning,
    feature engineering, and more. These stages are used to track the current
    phase of data processing within a pipeline or service.

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
        Represents core data processing tasks that are used across multiple stages,
        labeled as "99_core".
    """

    RAW = "00_raw"
    INGEST = "01_ingest"
    DQA = "02_dqa"
    CLEAN = "03_clean"
    FEATURE = "04_features"
    TEXTFEATURE = "05_text_features"
    AGGTRICS = "06_agg_metrics"
    CORE = "99_core"  # Doesn't belong to a specific stage; used in multiple stages.


# ------------------------------------------------------------------------------------------------ #
class Phase(Enum):
    PREP = 1
    ANALYSIS = 2
