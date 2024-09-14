#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/base/identity.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 11:31:20 pm                                              #
# Modified   : Saturday September 14th 2024 05:21:42 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from abc import ABC, abstractmethod


# ------------------------------------------------------------------------------------------------ #
class IDXGen(ABC):
    """
    Abstract base class for generating unique identifiers.

    This class defines the interface for generating unique IDs for a given owner.
    Subclasses are responsible for implementing the actual ID generation logic,
    which could include strategies such as UUIDs, sequential IDs, or custom formats.

    Methods:
    --------
    get_next_id(owner):
        Generates the next unique identifier for the given owner type.
        Subclasses must provide the specific logic for generating the ID.
    """

    @abstractmethod
    def get_next_id(self, owner: type) -> str:
        """
        Generates and returns the next unique identifier for a given owner.

        Parameters:
        -----------
        owner : type
            The class or type for which the ID is being generated.

        Returns:
        --------
        str:
            A unique identifier for the owner.

        This method must be implemented by subclasses to define specific ID generation
        strategies, such as UUIDs, sequential IDs, or other formats.
        """
        pass
