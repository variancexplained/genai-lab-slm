#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/utils/env.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 05:45:28 pm                                                   #
# Modified   : Friday June 7th 2024 01:17:05 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os

from dotenv import dotenv_values, load_dotenv


# ------------------------------------------------------------------------------------------------ #
class EnvManager:
    """
    A class for managing environment variables and .env file.

    Attributes:
        _file_path (str): Path to the .env file.
        _current_environment (str): Current environment variable value.
        _observers (list): List of observers to notify on environment change.
    """

    def __init__(self, file_path: str = ".env"):
        """
        Initialize the EnvManager with the path to the .env file.

        Args:
            file_path (str): Path to the .env file.
        """
        self._file_path = file_path
        self._current_environment = self.get_environment()
        self._observers = []

    def register_observer(self, observer):
        """
        Register an observer to receive notifications on environment change.

        Args:
            observer: The observer to register.
        """
        self._observers.append(observer)

    def notify_observers(self):
        """
        Notify all registered observers about environment change.
        """
        for observer in self._observers:
            observer.reset_cache()

    def change_environment(self, new_value: str) -> None:
        """
        Changes the environment variable and updates it in the current process.

        Args:
            new_value (str): The new value to set for the key.
        """
        key = "ENV"
        # Load existing values
        env_values = dotenv_values(self._file_path)
        # Add/update the key-value pair
        env_values[key] = new_value
        # Write all values back to the file
        with open(self._file_path, "w") as file:
            for k, v in env_values.items():
                file.write(f"{k}={v}\n")
        # Update the environment variable in the current process
        os.environ[key] = new_value
        # Notify observers to refresh cache.
        self.notify_observers()
        print(f"Updated {key} to {new_value} in {self._file_path} and current process")

    def get_environment(self):
        """
        Gets the environment variable

        Returns:
            str: The value of the environment variable.
        """
        return os.getenv("ENV")

    def load_environment(self):
        """
        Load environment variables from the .env file.
        """
        load_dotenv(self._env_file, override=True)
