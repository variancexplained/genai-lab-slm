#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/exception/dataset.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 01:00:01 am                                                #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
class DatasetNotFoundError(Exception):
    """Exception raised when a dataset is not found."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)


# ------------------------------------------------------------------------------------------------ #
class DatasetExistsError(Exception):
    """Exception raised when a dataset already exists."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)


# ------------------------------------------------------------------------------------------------ #
class DatasetCreationError(Exception):
    """Exception raised during dataset creation operations."""

    def __init__(self, msg: str = None, exc: Exception = None):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc


# ------------------------------------------------------------------------------------------------ #
class DatasetRemovalError(Exception):
    """Exception raised during dataset removal operations."""

    def __init__(self, msg: str = None, exc: Exception = None):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc


# ------------------------------------------------------------------------------------------------ #
class DatasetIOError(Exception):
    """Exception raised during dataset IO operations."""

    def __init__(self, msg: str = None, exc: Exception = None):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc


# ------------------------------------------------------------------------------------------------ #
class DatasetIntegrityError(Exception):
    """Exception raised when a dataset object exists, but the corresponding file is not found."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)


# ------------------------------------------------------------------------------------------------ #
class DatasetBuilderError(Exception):
    """Exception raised in DatasetBuilder."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)
