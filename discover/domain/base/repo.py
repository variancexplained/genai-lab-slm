#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/base/repo.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 9th 2024 03:00:19 pm                                               #
# Modified   : Wednesday September 18th 2024 05:14:22 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Persistence base class module"""
from abc import ABC, abstractmethod
from typing import Any, Dict

from discover.application.service.base.config import DataConfig


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


# ------------------------------------------------------------------------------------------------ #
class RepoFactory(ABC):
    """
    Abstract base class for repository factories.

    The purpose of this class is to define a factory interface for creating repository
    instances based on the provided configuration. Concrete implementations of this
    factory will create specific types of repositories depending on the data source,
    database, or other storage mechanisms specified in the configuration.

    Methods:
    --------
    get_repo(config: DataConfig) -> Repo:
        Abstract method that, when implemented, returns a repository instance based
        on the given configuration.

        Parameters:
        -----------
        config : DataConfig
            Configuration object containing necessary information to initialize
            the repository (e.g., database URL, credentials, etc.).

        Returns:
        --------
        Repo:
            An instance of a repository that is configured according to the
            provided configuration.
    """

    @abstractmethod
    def get_repo(self, config: DataConfig) -> Repo:
        """Returns a repository based upon the configuration provided."""
