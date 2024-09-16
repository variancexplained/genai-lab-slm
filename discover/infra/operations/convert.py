#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/convert.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 01:40:18 pm                                                   #
# Modified   : Monday September 16th 2024 01:47:24 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""IO Module"""
from typing import Union

import pandas as pd
from pyspark.sql import DataFrame

from discover.domain.base.task import Task
from discover.domain.service.core.monitor.profiler import profiler
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.utils.data_utils.converter import Converter
from discover.infra.utils.file_utils.tempfile import TempFileMgr


# ------------------------------------------------------------------------------------------------ #
#                                    CONVERT TASK                                                  #
# ------------------------------------------------------------------------------------------------ #
class ConvertTask(Task):
    """Converts DataFrames between Pandas and Spark DataFrame formats.

    Args:
        converter_cls (type[Converter]): A Converter converter class
        tempfile_manager_cls (type[TempFileMgr]): Tempfile manager
        **kwargs: Other keyword arguments.
    """

    __stage = Stage.CORE

    def __init__(
        self,
        converter_cls: type[Converter],
        tempfile_manager_cls: type[TempFileMgr] = TempFileMgr,
        **kwargs,
    ) -> None:
        super().__init__(converter_cls=converter_cls, stage=Stage.CORE)
        self._converter_cls = converter_cls
        self._tempfile_manager_cls = tempfile_manager_cls
        self._kwargs = kwargs

    @profiler
    @announcer
    def run(
        self, data: Union[pd.DataFrame, DataFrame]
    ) -> Union[pd.DataFrame, DataFrame]:
        """Converts a Pandas DataFrame to a Spark DataFrame

        Args:
            data (pd.DataFrame): Pandas DataFrame
        """

        converter = self._converter_cls(
            tempfile_manager_cls=self._tempfile_manager_cls, **self._kwargs
        )
        data = converter.convert(data=data)

        return data
