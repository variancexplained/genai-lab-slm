#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/dal/base/location.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday October 16th 2024 10:22:35 pm                                             #
# Modified   : Wednesday October 16th 2024 10:24:20 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod


# ------------------------------------------------------------------------------------------------ #
class LocationService(ABC):
    """
    Abstract base class for defining a location service that provides file path generation.
    This class is intended to be inherited by concrete implementations that define how file
    paths are constructed based on specific parameters.

    Methods:
        get_filepath(*args, **kwargs) -> str:
            Abstract method to generate a file path. Concrete subclasses must implement this
            method to provide the logic for constructing file paths based on various arguments.
    """

    @abstractmethod
    def get_filepath(self, *args, **kwargs) -> str:
        """
        Abstract method to generate a file path based on the provided arguments.
        This method should be implemented by subclasses to define the logic for constructing
        file paths according to the needs of the specific implementation.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            str: The generated file path as a string.
        """
        pass
