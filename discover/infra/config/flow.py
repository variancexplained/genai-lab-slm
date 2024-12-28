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
# Modified   : Friday December 27th 2024 05:20:59 pm                                               #
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
    A configuration reader for loading and merging orchestration configurations.

    This class extends `ConfigReader` to handle the loading and merging of
    base and environment-specific configuration files. It provides an option to
    control how lists are merged or overridden when merging configurations.

    Attributes:
        env_file_path (str): The file path to the environment-specific configuration file.
            Defaults to ".env".
        list_override (bool): A flag that determines whether lists in the environment-specific
            configuration should override the lists in the base configuration. If set to `True`,
            lists in the environment-specific configuration will replace those in the base
            configuration. If set to `False`, lists will be merged. Defaults to `True`.
    """

    def __init__(self, env_file_path: str = ".env", list_override: bool = True):
        super().__init__(env_file_path=env_file_path, list_override=list_override)

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
