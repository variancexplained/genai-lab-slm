#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/config/config.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday July 19th 2024 08:27:38 am                                                   #
# Modified   : Monday September 9th 2024 09:32:17 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Configuration Classes."""
# %%
import logging
import os
from typing import Any, Dict, Optional, Union, cast

import yaml
from discover.core.data import NestedNamespace
from dotenv import dotenv_values, load_dotenv

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
#                                       CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
class Config:
    """
    A class for managing configuration and .env environment variables.

    Attributes:
        _env_file_path (str): Path to the .env file.
        _current_environment (str): Current environment variable value.
        namespace_mode (bool): If True, returneed data can be accessed using dot
            notation.Default = True
    """

    def __init__(self, env_file_path: str = ".env"):
        """
        Initialize the Config class with the path to the .env file.

        Args:
            file_path (str): Path to the .env file.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._env_file_path = env_file_path
        self._current_environment = self.get_environment()
        self._config = self.load_config()

    #  ------------------------------------------------------------------------------------------- #
    def get_config(
        self, section: Optional[str] = None, namespace: bool = True
    ) -> Union[Dict[str, Any], NestedNamespace, str]:
        """
        Retrieves configuration data either as a dictionary or a namespace object.

        Args:
            section (Optional[str]): The section of the configuration to retrieve.
                                    If None, the entire configuration is returned.
            namespace (bool): If True, returns the configuration as a NestedNamespace object.
                            If False, returns the configuration as a dictionary.

        Returns:
            Union[Dict[str, Any], NestedNamespace, str]:
                - If `section` is provided, returns the configuration for the specific section.
                - If `namespace` is True, the configuration is returned as a NestedNamespace.
                - If `namespace` is False, returns the configuration as a dictionary.

        Raises:
            KeyError: If the specified section is not found in the configuration.
            Exception: For any other unknown exceptions that occur during retrieval.

        Notes:
            - Logs exceptions when no section is found or when an unknown error occurs.
        """

        if section is None:
            config = self._config
        else:
            try:
                config = self._config[section]
            except KeyError:
                msg = f"No configuration found for the {section} section."
                self._logger.exception(msg)
                raise
            except Exception as e:
                msg = f"Unknown exception occurred.\n{e}"
                self._logger.exception(msg)
                raise
        if namespace:
            return self.to_namespace(config)
        else:
            return config

    #  ------------------------------------------------------------------------------------------- #
    @property
    def aws(self) -> NestedNamespace:
        config = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region_name": os.getenv("AWS_REGION_NAME"),
        }
        return self.to_namespace(config)

    #  ------------------------------------------------------------------------------------------- #
    @property
    def current_environment(self) -> str:
        return self._current_environment

    #  ------------------------------------------------------------------------------------------- #
    def change_environment(self, new_value: str) -> None:
        """
        Changes the environment variable and updates it in the current process.

        Args:
            new_value (str): The new value to set for the key.
        """
        key = "ENV"
        # Load existing values
        env_values = dotenv_values(self._env_file_path)
        # Add/update the key-value pair
        env_values[key] = new_value
        # Write all values back to the file
        with open(self._env_file_path, "w") as file:
            for k, v in env_values.items():
                file.write(f"{k}={v}\n")
        # Update the environment variable in the current process
        os.environ[key] = new_value
        # Load the current environment
        self.load_environment()
        print(
            f"Updated {key} to {new_value} in {self._env_file_path} and current process"
        )

    #  ------------------------------------------------------------------------------------------- #
    def get_environment(self) -> str:
        """
        Gets the environment

        Returns:
            str: The value of the environment.
        """
        return os.getenv("ENV", "dev")

    #  ------------------------------------------------------------------------------------------- #
    def load_environment(self) -> None:
        """
        Load environment variables from the .env file.
        """
        load_dotenv(self._env_file_path, override=True)

    #  ------------------------------------------------------------------------------------------- #
    def get_env_var(self, key: str) -> Optional[str]:
        """
        Gets an environment variable

        Returns:
            str: The value of the requested environmet variable.
        """
        return os.getenv(key)

    #  ------------------------------------------------------------------------------------------- #
    def load_config(self) -> Dict[str, Any]:
        """
        Loads the base configuration as well as the environment-specific configuration.

        This method merges the base configuration with environment-specific settings
        by loading them separately and then combining them into a single dictionary.

        Returns:
            Dict[str, Any]: A dictionary containing the combined configuration, with
                            environment-specific settings overriding the base settings.
        """
        base_config = self._load_base_config()
        env_config = self._load_env_config()
        return {**base_config, **env_config}

    #  ------------------------------------------------------------------------------------------- #
    def _load_base_config(self) -> Dict[str, Any]:
        """
        Loads the base configuration from a YAML file.

        The file path is determined by the `CONFIG_BASE_FILEPATH` environment variable,
        or it defaults to `config/base.yaml`. This method reads the YAML file and returns
        its contents as a dictionary.

        Returns:
            Dict[str, Any]: The base configuration as a dictionary.

        Raises:
            FileNotFoundError: If the base configuration file is not found.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """
        base_config = {}

        directory = os.getenv("CONFIG_DIRECTORY", "config")
        filepath = os.path.join(directory, "base.yaml")
        content = "base configuration"
        base_config = self.read_yaml(filepath=filepath, content=content)
        return base_config

    #  ------------------------------------------------------------------------------------------- #
    def _load_env_config(self) -> Dict[str, Any]:
        """
        Loads the environment-specific configuration from a YAML file.

        The file path is determined by the `filepath` attribute of the class. If the
        `filepath` is not set, a `RuntimeError` is raised. This method reads the YAML
        file and returns its contents as a dictionary.

        Returns:
            Dict[str, Any]: The environment-specific configuration as a dictionary.

        Raises:
            RuntimeError: If the environment-specific configuration file path is not set.
            FileNotFoundError: If the environment-specific configuration file is not found.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """
        directory = os.getenv("CONFIG_DIRECTORY", "config")
        filepath = os.path.join(directory, f"{self.current_environment}.yaml")
        content = f"{self.current_environment} environment configuration"
        env_config = self.read_yaml(filepath=filepath, content=content)
        return env_config

    #  ------------------------------------------------------------------------------------------- #
    def read_yaml(self, filepath: str, content: str) -> Dict[str, Any]:
        """
        Reads a YAML file and returns its contents as a dictionary.

        This method attempts to load a YAML file from the specified `filepath`. If the
        file is found and successfully parsed, its contents are returned as a dictionary.
        If the file is not found or there is an error parsing the YAML content, an exception
        is logged and re-raised.

        Args:
            filepath (str): The path to the YAML file to be read.
            content (str): A description of the content being read, used for logging purposes.

        Returns:
            Dict[str, Any]: The contents of the YAML file as a dictionary.

        Raises:
            FileNotFoundError: If the specified file is not found at the given `filepath`.
            yaml.YAMLError: If there is an error parsing the YAML file.
        """
        try:
            with open(filepath, "r") as file:
                data = yaml.safe_load(file)
                return cast(Dict[str, Any], data)
        except FileNotFoundError:
            self._logger.exception(
                f"Unable to read {content}. File not found at {filepath}."
            )
            raise
        except yaml.YAMLError as e:
            self._logger.exception(
                f"Exception while reading {content} from {filepath}\n{e}"
            )
            raise

    #  ------------------------------------------------------------------------------------------- #
    def to_namespace(self, config: Dict[str, Any]) -> NestedNamespace:
        """
        Converts a configuration dictionary to a NestedNamespace object.

        This method transforms a flat or nested dictionary of configuration settings
        into a `NestedNamespace` object, which allows for attribute-style access to
        the configuration values.

        Args:
            config (Dict[str, Any]): The configuration dictionary to be converted.

        Returns:
            NestedNamespace: The configuration wrapped in a `NestedNamespace` object.
        """
        return NestedNamespace(config)


#  ------------------------------------------------------------------------------------------- #
# Usage
# config = Config()
# dbconfig = config.get_config(section="database")
# print(dbconfig)
