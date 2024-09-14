#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/service/base/identity.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 11:31:20 pm                                              #
# Modified   : Friday September 13th 2024 11:33:39 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod


# ------------------------------------------------------------------------------------------------ #
class IDXGen(ABC):
    @abstractmethod
    def get_next_id(self, owner: type) -> str:
        """
        Generates and returns the next unique identifier for a given owner class.

        Args:
            owner (type): The class or type for which the ID is being generated.

        Returns:
            str: A unique identifier as a string.

        This method should be implemented by subclasses to provide specific ID generation
        strategies, such as UUID, sequential, or custom generators.
        """
