#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/dataset/config.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from dataclasses import field
from typing import Optional

from pydantic.dataclasses import dataclass

from genailab.asset.base.asset import AssetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                   DATASET CONFIG                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetConfig(AssetConfig):
    """Configuration for datasets, inheriting from AssetConfig.

    Extends the base asset configuration by adding a specification
    for the dataset type.

    """
    dftype: Optional[DFType] = field(default=None)

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
            dftype = DFType.from_value(config['dftype'])

            return cls(
                phase=phase,
                stage=stage,
                name=name,
                file_format=file_format,
                dftype=dftype,
                description=config.get("description", None),
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid dataset configuration: {e}")
