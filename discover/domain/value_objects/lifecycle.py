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
# Modified   : Friday September 13th 2024 05:09:22 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class Stage(Enum):
    RAW = "00_raw"
    INGEST = "01_ingest"
    DQA = "02_dqa"
    CLEAN = "03_clean"
    FEATURE = "04_features"
    TEXTFEATURE = "05_text_features"
    AGGTRICS = "06_agg_metrics"
    CORE = "99_core"  # Doesn't belong to a stage. Used in multiple stages


# ------------------------------------------------------------------------------------------------ #
class Phase(Enum):
    PREP = 1
    ANALYSIS = 2
