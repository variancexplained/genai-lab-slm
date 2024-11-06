#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/config/flow.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday July 19th 2024 08:27:38 am                                                   #
# Modified   : Tuesday November 5th 2024 05:47:34 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Configuration Classes."""

import os
from typing import Any, Dict

from dotenv import load_dotenv

from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.base import ConfigReader

# ------------------------------------------------------------------------------------------------ #
load_dotenv()

# ------------------------------------------------------------------------------------------------ #
#                              ORCHESTRATION CONFIG READER                                         #
# ------------------------------------------------------------------------------------------------ #


class FlowConfigReader(ConfigReader):
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
        super().__init__(env_file_path=env_file_path)

    def get_stage_config(self, phase: PhaseDef, stage: StageDef) -> dict:
        """Returns the configuration for a workflow stage

        Args:
            phase (PhaseDef): Workflow phase
            stage (StageDef): Workflow stage within the phase.
        """
        phase = phase.value
        stage = stage.value
        config = self.get_config(section="phases", namespace=False)
        return config[phase]["stages"][stage]

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
        filepath = os.path.join(directory, "base", "flow.yaml")
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
        filepath = os.path.join(directory, self.current_environment, "flow.yaml")
        return self.read_yaml(
            filepath=filepath,
            content=f"{self.current_environment} environment configuration",
        )


# ------------------------------------------------------------------------------------------------ #
