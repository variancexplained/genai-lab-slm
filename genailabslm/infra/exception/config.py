#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/exception/config.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 01:00:01 am                                                #
# Modified   : Saturday January 25th 2025 04:44:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
class PhaseConfigurationError(Exception):
    """Exception raised when a dataset is not found."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)


# ------------------------------------------------------------------------------------------------ #
class StageConfigurationException(Exception):
    """Exception raised for invalid stage configuration."""

    def __init__(self, msg: str, exc: Exception):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc


# ------------------------------------------------------------------------------------------------ #
class DatasetConfigurationException(Exception):
    """Exception raised for invalid dataset configuration."""

    def __init__(self, msg: str, exc: Exception):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc
