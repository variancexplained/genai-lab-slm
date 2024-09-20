#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/config/service.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Friday September 20th 2024 01:03:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

from dataclasses import dataclass

from discover.domain.entity.config.base import Config
from discover.domain.entity.config.dataset import DatasetConfig


# ------------------------------------------------------------------------------------------------ #
#                                       SERVICE CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(Config):
    """
    Configuration class for managing service-related settings in the pipeline.

    This class handles the configuration of source and target data within the service and includes
    an optional flag, `force`, which determines whether tasks should be re-executed even if they
    were completed previously.

    Attributes:
    -----------
    source_data_config : DatasetConfig
        The configuration for the source data, defining how the input data is handled.

    target_data_config : DatasetConfig
        The configuration for the target data, specifying where and how the processed data will be stored.

    force : bool, default=False
        A flag indicating whether to force the re-execution of tasks, even if they have been previously completed.

    Methods:
    --------
    validate() -> None:
        Validates the service configuration by ensuring that `source_data_config` and `target_data_config`
        are valid instances of `DatasetConfig`, and that `force` is a boolean value. It also validates
        the `source_data_config` and `target_data_config` by invoking their respective `validate()` methods.
        Raises an `InvalidConfigException` if any attribute is invalid.
    """

    source_data_config: DatasetConfig
    target_data_config: DatasetConfig
    force: bool = False

    def _validate(self) -> list:
        """
        Validates the ServiceConfig.

        This method checks:
        - `source_data_config` is an instance of `DatasetConfig`.
        - `target_data_config` is an instance of `DatasetConfig`.
        - `force` is a boolean value.

        It also calls the `validate()` method of both `source_data_config` and `target_data_config` to ensure
        they are valid configurations themselves. If any validation fails, an `InvalidConfigException` is raised
        with a detailed error message.

        Raises:
        -------
        InvalidConfigException:
            If any of the configuration attributes are invalid, or if validation of the `source_data_config`
            or `target_data_config` fails.
        """
        errors = super()._validate()

        if not isinstance(self.source_data_config, DatasetConfig):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DatasetConfig instance. Encountered {type(self.source_data_config).__name__}."
            )
        if not isinstance(self.target_data_config, DatasetConfig):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DatasetConfig instance. Encountered {type(self.target_data_config).__name__}."
            )
        if not isinstance(self.force, bool):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a boolean type. Encountered {type(self.force).__name__}."
            )

        return errors
