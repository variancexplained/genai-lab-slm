#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/base/repo.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 03:00:19 pm                                               #
# Modified   : Saturday February 8th 2025 10:43:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Repository Interface Module"""
from abc import ABC, abstractmethod
from typing import Any, Dict


# ------------------------------------------------------------------------------------------------ #
#                                          REPO                                                    #
# ------------------------------------------------------------------------------------------------ #
class Repo(ABC):
    """
    Abstract base class for persistence repositories.

    This class defines the interface for interacting with a persistence layer. It provides
    methods for adding, retrieving, removing, and checking the existence of data. Subclasses
    must implement these methods to handle the actual persistence logic, such as interacting
    with databases, file systems, or other storage mechanisms.

    Methods:
    --------
    add(*args, **kwargs):
        Persists data to the repository. Must be implemented by subclasses.

    get(*args, **kwargs):
        Retrieves data from the repository based on the given criteria. Must be implemented by subclasses.

    remove(*args, **kwargs):
        Deletes data from the repository based on the given criteria. Must be implemented by subclasses.

    exists(*args, **kwargs):
        Checks if data exists in the repository based on the given criteria. Must be implemented by subclasses.
    """

    @abstractmethod
    def add(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        """
        Persists data to the repository.

        Parameters:
        -----------
        *args : Any
            Positional arguments specifying the data to persist.
        **kwargs : Dict[str, Any]
            Keyword arguments specifying additional details for persistence.

        Raises:
        -------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        pass

    @abstractmethod
    def get(self, *args: Any, **kwargs: Dict[str, Any]) -> Any:
        """
        Retrieves data from the repository.

        Parameters:
        -----------
        *args : Any
            Positional arguments specifying criteria for retrieving data.
        **kwargs : Dict[str, Any]
            Keyword arguments specifying additional criteria for data retrieval.

        Returns:
        --------
        Any:
            The retrieved data.

        Raises:
        -------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        pass

    @abstractmethod
    def remove(self, *args: Any, **kwargs: Dict[str, Any]) -> None:
        """
        Deletes data from the repository.

        Parameters:
        -----------
        *args : Any
            Positional arguments specifying the data to be removed.
        **kwargs : Dict[str, Any]
            Keyword arguments specifying additional criteria for data removal.

        Raises:
        -------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        pass

    @abstractmethod
    def exists(self, *args: Any, **kwargs: Dict[str, Any]) -> bool:
        """
        Checks if data exists in the repository.

        Parameters:
        -----------
        *args : Any
            Positional arguments specifying the criteria for checking data existence.
        **kwargs : Dict[str, Any]
            Keyword arguments specifying additional criteria for the check.

        Returns:
        --------
        bool:
            True if the data exists, False otherwise.

        Raises:
        -------
        NotImplementedError
            If the method is not implemented by a subclass.
        """
        pass

    def get_all(self, *args: Any, **kwargs: Dict[str, Any]) -> dict:
        """Retrieves all assets in the repository.

        Returns:
            Dict[str, Asset]: A dictionary of all assets, where keys are asset IDs
            and values are Asset objects.
        """
        pass
