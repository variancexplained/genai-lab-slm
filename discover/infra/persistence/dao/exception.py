#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/dal/dao/exception.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 01:00:01 am                                                #
# Modified   : Friday October 11th 2024 06:41:52 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
class ObjectNotFoundError(Exception):
    """Exception raised when a dataset is not found."""

    def __init__(self, msg: str = None):
        self.msg = msg
        super().__init__(msg)


# ------------------------------------------------------------------------------------------------ #
class ObjectIOException(Exception):
    """Exception raised for errors during object I/O operations."""

    def __init__(self, msg: str, exc: Exception):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc


# ------------------------------------------------------------------------------------------------ #
class ObjectDatabaseNotFoundError(Exception):
    """Exception raised when the object database is not found."""

    def __init__(self, msg: str, exc: Exception):
        super().__init__(f"{msg}\nOriginal exception: {exc}")
        self.exc = exc
