#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/value_objects/enum.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:26:24 am                                                    #
# Modified   : Wednesday September 11th 2024 11:25:06 am                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class Stage(Enum):
    RAW = "00_raw"
    NORM = "01_norm"
    DQA = "02_dqa"
    CLEAN = "03_clean"
    FEATURE = "04_features"
    NLP = "05_nlp"
    METRICS = "06_metrics"
    MULTI = "99_multi"  # Doesn't belong to a stage. Used in multiple stages


# ------------------------------------------------------------------------------------------------ #
class Phase(Enum):
    PREP = 1
    ANALYSIS = 2
