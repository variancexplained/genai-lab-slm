#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/asset/dataset/config.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Sunday January 26th 2025 05:56:36 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from genailabslm.asset.base.asset import AssetConfig
from genailabslm.core.flow import PhaseDef, StageDef
from genailabslm.infra.utils.file.fileset import FileFormat
from pydantic.dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                   DATASET CONFIG                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetConfig(AssetConfig):
    """Configuration for datasets, inheriting from AssetConfig.

    Extends the base asset configuration by adding a specification
    for the dataset type.

    """

    @classmethod
    def from_dict(cls, config: dict) -> DatasetConfig:
        """
        Creates a DatasetConfig object from a dictionary, deserializing
        strings to their Enum values.

        Args:
            config: A dictionary containing the dataset configuration.

        Returns:
            DatasetConfig: The created DatasetConfig object.

        Raises:
            ValueError: If the configuration is invalid.
        """
        try:
            phase = PhaseDef.from_value(config["phase"])
            stage = StageDef.from_value(config["stage"])
            name = config["name"]
            file_format = FileFormat.from_value(config["file_format"])

            return cls(
                phase=phase,
                stage=stage,
                name=name,
                file_format=file_format,
                description=config.get("description", None),
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid dataset configuration: {e}")
