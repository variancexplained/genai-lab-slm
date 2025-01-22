#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/state.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 01:20:36 am                                             #
# Modified   : Wednesday January 22nd 2025 01:23:03 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
from enum import Enum


# ------------------------------------------------------------------------------------------------ #
class DatasetState(Enum):
    CREATED = "created"  # Created, not yet published
    PUBLISHED = "published"  # Published in the repository
    CONSUMED = (
        "consumed"  # Consumed by the a data processing, analysis, or modeling pipeline.
    )
    REMOVED = "removed"  # The dataset is removed from the repository.
