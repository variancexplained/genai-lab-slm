#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/task/enrich/base.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday November 21st 2024 11:21:06 pm                                             #
# Modified   : Thursday November 21st 2024 11:58:58 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from discover.flow.task.base import Task


# ------------------------------------------------------------------------------------------------ #
#                                      ANOMALY                                                     #
# ------------------------------------------------------------------------------------------------ #
class EnrichmentTask(Task):
    """
    Base class for handling data enrichment.

    Args:
        column (str): The name of the column to analyze.
        new_column (str): The name of the column to store detection or repair results.
        **kwargs: Additional arguments for specific anomaly configurations.

    """

    def __init__(
        self,
        column: str,
        new_column: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self._column = column
        self._new_column = f"{self.stage.id}_{new_column}"
        self._kwargs = kwargs
