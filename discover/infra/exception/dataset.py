#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/exception.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 01:00:01 am                                                #
# Modified   : Saturday October 12th 2024 12:08:11 am                                              #
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
