#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/ingest.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:19:05 am                                              #
# Modified   : Thursday October 17th 2024 01:13:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import pandas as pd

from discover.orchestration.base.task import Task


# ------------------------------------------------------------------------------------------------ #
class IngestTask(Task):
    def __init__(self, frac: float, random_state: int = None) -> None:
        super().__init__()
        self._frac = frac
        self._random_state = random_state

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        return data.sample(frac=self._frac, random_state=self._random_state)
