#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/value_objects/context.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 02:12:54 pm                                              #
# Modified   : Saturday September 14th 2024 05:35:28 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass

from discover.core.data import DataClass
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Context(DataClass):
    """
    A dataclass that represents the execution context for a task or service, containing
    essential metadata like the service type, service name, and the current stage of the
    pipeline.

    Attributes:
    -----------
    process_type : str
        The type of service being executed (e.g., 'data processing', 'data extraction').

    process_name : str
        The name of the specific service or task being run.

    stage : Stage
        The current stage of the pipeline where the task or service is operating.
    """

    process_type: str
    process_name: str
    stage: Stage
