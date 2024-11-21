#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/clean/text/anomaly.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 01:31:19 am                                             #
# Modified   : Thursday November 21st 2024 03:20:34 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from discover.flow.task.clean.base.anomaly import Anomaly


class TextAnomaly(Anomaly):
    def __init__(
        self,
        column: str,
        new_column: str,
        mode: str,
        detect_strategy: str,
        repair_strategy: str,
        strategy_factory: Type[TextStrategyFactory],
        **kwargs,
    ):
        super().__init__(
            column,
            new_column,
            mode,
            detect_strategy,
            repair_strategy,
            strategy_factory,
            **kwargs,
        )
