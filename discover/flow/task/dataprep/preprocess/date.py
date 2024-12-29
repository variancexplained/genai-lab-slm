#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/dataprep/preprocess/date.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday December 28th 2024 07:50:01 pm                                             #
# Modified   : Saturday December 28th 2024 08:22:20 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Date Preprocessing Module"""
import pandas as pd
import pytz
from pandarallel import pandarallel

from discover.flow.task.base import Task
from discover.infra.service.logging.task import task_logger

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class ConvertDateTimeUTC(Task):

    def __init__(
        self, column: str = "date", timezone: str = "America/New_York", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self._column = column
        self._kwargs = kwargs
        self._timezone = timezone

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        data[self._column] = data[self._column].parallel_apply(self._convert_to_utc)
        return data

    def convert_to_utc(self, ts):
        local_tz = pytz.timezone(self._timezone)
        try:
            # Localize and convert to UTC
            return local_tz.localize(ts, is_dst=None).astimezone(pytz.utc)
        except pytz.NonExistentTimeError:
            # Handle non-existent times by shifting forward
            return local_tz.normalize(ts + pd.Timedelta(hours=1)).astimezone(pytz.utc)
        except pytz.AmbiguousTimeError:
            # Handle ambiguous times (default to standard time)
            return local_tz.localize(ts, is_dst=False).astimezone(pytz.utc)
