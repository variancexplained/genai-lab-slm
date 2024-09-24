#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/config/reader.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday July 19th 2024 08:27:38 am                                                   #
# Modified   : Tuesday September 24th 2024 02:02:42 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Configuration Classes."""
# %%
import logging
import os
from typing import Any, Dict, Optional, Union

import yaml
from dotenv import dotenv_values, load_dotenv

from discover.core.namespace import NestedNamespace

# ------------------------------------------------------------------------------------------------ #
load_dotenv()

# ------------------------------------------------------------------------------------------------ #
#                                       CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #


class ConfigReader:
    """
    A class for managing configuration and environment variables.

    This class provides methods to load and manage configuration files in YAML format
    and environment variables from `.env` files. It also supports retrieving specific
    configuration sections, converting configuration dictionaries into dot-accessible
    namespaces, and managing AWS-related configuration settings.

    Attributes:
    -----------
    _env_file_path : str
        The file path to the `.env` file that holds environment variables.
    _current_environment : str
        The current environment (e.g., "dev", "prod"), which is loaded from the `.env` file.
    _config : dict
        The combined configuration loaded from both the base and environment-specific YAML files.

    Methods:
    --------
    get_config(section: Optional[str] = None, namespace: bool = True) -> Union[Dict[str, Any], NestedNamespace, str]:
        Retrieves the entire configuration or a specific section as a dictionary or `NestedNamespace` object.

    aws() -> NestedNamespace:
        Returns AWS credentials and region as a `NestedNamespace` object.

    current_environment() -> str:
        Returns the current environment value loaded from the `.env` file.

    change_environment(new_value: str) -> None:
        Updates the current environment variable in both the `.env` file and the current process environment.

    get_environment() -> str:
        Retrieves the current environment from the environment variables or defaults to "dev" if not set.

    load_environment() -> None:
        Loads environment variables from the `.env` file, overriding any existing environment variables.

    get_env_var(key: str) -> Optional[str]:
        Retrieves the value of a specific environment variable.

    load_config() -> Dict[str, Any]:
        Loads and merges the base configuration and environment-specific configuration files.

    _load_base_config() -> Dict[str, Any]:
        Loads the base configuration from a YAML file.

    _load_env_config() -> Dict[str, Any]:
        Loads the environment-specific configuration from a YAML file based on the current environment.

    read_yaml(filepath: str, content: str) -> Dict[str, Any]:
        Reads a YAML file and returns its contents as a dictionary, logging errors if the file is not found or invalid.

    to_namespace(config: Dict[str, Any]) -> NestedNamespace:
        Converts a configuration dictionary to a `NestedNamespace` object, allowing dot-access notation for settings.
    """

    def __init__(self, env_file_path: str = ".env"):
        """
        Initializes the Config class with the path to the `.env` file.

        Parameters:
        -----------
        env_file_path : str
            The file path to the `.env` file that holds environment variables.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._env_file_path = env_file_path
        self._current_environment = self.get_environment()
        self._config = self.load_config()

    def get_config(
        self, section: Optional[str] = None, namespace: bool = True
    ) -> Union[Dict[str, Any], NestedNamespace, str]:
        """
        Retrieves configuration data either as a dictionary or as a `NestedNamespace` object.

        Parameters:
        -----------
        section : Optional[str]
            The section of the configuration to retrieve. If None, the entire configuration is returned.
        namespace : bool
            If True, returns the configuration as a `NestedNamespace` object. Defaults to True.

        Returns:
        --------
        Union[Dict[str, Any], NestedNamespace, str]:
            The entire configuration or the specified section as a dictionary or `NestedNamespace` object.

        Raises:
        -------
        KeyError:
            If the requested section is not found in the configuration.
        """
        config = self._config[section] if section else self._config
        return self.to_namespace(config) if namespace else config

    @property
    def aws(self) -> NestedNamespace:
        """
        Returns AWS credentials and region configuration as a `NestedNamespace`.

        The AWS configuration includes the `aws_access_key_id`, `aws_secret_access_key`,
        and `region_name` retrieved from the environment variables.

        Returns:
        --------
        NestedNamespace:
            The AWS configuration in dot-notation accessible form.
        """
        config = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region_name": os.getenv("AWS_REGION_NAME"),
        }
        return self.to_namespace(config)

    @property
    def current_environment(self) -> str:
        """
        Returns the current environment value loaded from the `.env` file.

        Returns:
        --------
        str:
            The current environment value (e.g., "dev", "prod").
        """
        return self._current_environment

    def change_environment(self, new_value: str) -> None:
        """
        Updates the environment value in the `.env` file and the current process.

        Parameters:
        -----------
        new_value : str
            The new environment value to set (e.g., "prod", "test").

        Notes:
        ------
        This method modifies the `.env` file and updates the `ENV` environment variable
        in the current process to reflect the change.
        """
        key = "ENV"
        env_values = dotenv_values(self._env_file_path)
        env_values[key] = new_value
        with open(self._env_file_path, "w") as file:
            for k, v in env_values.items():
                file.write(f"{k}={v}\n")
        os.environ[key] = new_value
        self.load_environment()
        print(
            f"Updated {key} to {new_value} in {self._env_file_path} and current process"
        )

    def get_environment(self) -> str:
        """
        Retrieves the current environment value from the environment variables.

        Returns:
        --------
        str:
            The current environment value, defaulting to "dev" if not set.
        """
        return os.getenv("ENV", "dev")

    def load_environment(self) -> None:
        """
        Loads environment variables from the `.env` file, overriding any existing environment variables.
        """
        load_dotenv(self._env_file_path, override=True)

    def get_env_var(self, key: str) -> Optional[str]:
        """
        Retrieves the value of a specific environment variable.

        Parameters:
        -----------
        key : str
            The name of the environment variable to retrieve.

        Returns:
        --------
        Optional[str]:
            The value of the environment variable, or None if not set.
        """
        return os.getenv(key)

    def load_config(self) -> Dict[str, Any]:
        """
        Loads and merges the base configuration with environment-specific configuration files.

        Returns:
        --------
        Dict[str, Any]:
            The merged configuration from the base and environment-specific YAML files.
        """
        base_config = self._load_base_config()
        env_config = self._load_env_config()
        return {**base_config, **env_config}

    def _load_base_config(self) -> Dict[str, Any]:
        """
        Loads the base configuration from the `base.yaml` file.

        Returns:
        --------
        Dict[str, Any]:
            The base configuration settings.

        Raises:
        -------
        FileNotFoundError:
            Raised if the `base.yaml` file is not found.
        """
        directory = os.getenv("CONFIG_DIRECTORY", "config")
        filepath = os.path.join(directory, "base.yaml")
        return self.read_yaml(filepath=filepath, content="base configuration")

    def _load_env_config(self) -> Dict[str, Any]:
        """
        Loads the environment-specific configuration from a YAML file based on the current environment.

        Returns:
        --------
        Dict[str, Any]:
            The environment-specific configuration settings.

        Raises:
        -------
        FileNotFoundError:
            Raised if the environment-specific YAML file is not found.
        """
        directory = os.getenv("CONFIG_DIRECTORY", "config")
        filepath = os.path.join(directory, f"{self.current_environment}.yaml")
        return self.read_yaml(
            filepath=filepath,
            content=f"{self.current_environment} environment configuration",
        )

    def read_yaml(self, filepath: str, content: str) -> Dict[str, Any]:
        """
        Reads a YAML file and returns its contents as a dictionary.

        Parameters:
        -----------
        filepath : str
            The path to the YAML file.
        content : str
            Description of the content being read, used for logging purposes.

        Returns:
        --------
        Dict[str, Any]:
            The parsed contents of the YAML file.

        Raises:
        -------
        FileNotFoundError:
            Raised if the specified file is not found.
        yaml.YAMLError:
            Raised if there is an error parsing the YAML file.
        """
        try:
            with open(filepath, "r") as file:
                return yaml.safe_load(file)
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

    def to_namespace(self, config: Dict[str, Any]) -> NestedNamespace:
        """
        Converts a configuration dictionary into a `NestedNamespace` object.

        Parameters:
        -----------
        config : Dict[str, Any]
            The configuration dictionary to convert.

        Returns:
        --------
        NestedNamespace:
            The configuration wrapped in a `NestedNamespace` object, allowing dot notation access.
        """
        if isinstance(config, dict):
            return NestedNamespace(config)
        else:
            return config
