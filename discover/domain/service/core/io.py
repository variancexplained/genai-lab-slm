#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/core/io.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 07:13:32 pm                                              #
# Modified   : Saturday September 14th 2024 03:59:08 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Any

from discover.domain.service.base.task import Task
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
class ReadTask(Task):
    def __init__(self, data_config: DataConfig, context: Context) -> None:
        self._data_config = data_config
        self._context = context
        self._stage = context.stage

    def stage(self) -> Stage:
        return self._stage

    def run(self, *args, **kwargs) -> Any:
        super().run(args=args, **kwargs)
        return self._data_config.repo.get(
            stage=self._data_config.stage, name=self._data_config.name
        )


# ------------------------------------------------------------------------------------------------ #
class WriteTask(Task):

    def __init__(self, data_config: DataConfig, context: Context, **kwargs) -> None:
        self._data_config = data_config
        self._context = context
        self._stage = context.stage
        self._kwargs = kwargs

    def stage(self) -> Stage:
        return self._stage

    def run(self, data: Any) -> Any:
        super().run(data=data)
        self._data_config.repo.add(
            stage=self._data_config.stage,
            name=self._data_config.name,
            data=data,
            **self._kwargs,
        )
        return data
