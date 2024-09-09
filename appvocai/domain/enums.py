#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/domain/enums.py                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 01:26:24 am                                                    #
# Modified   : Monday July 1st 2024 01:47:42 am                                                    #
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


# ------------------------------------------------------------------------------------------------ #
class Phase(Enum):
    PREP = 1
    ANALYSIS = 2
