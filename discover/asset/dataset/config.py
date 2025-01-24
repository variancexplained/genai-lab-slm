#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/config.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:32:52 pm                                               #
# Modified   : Thursday January 23rd 2025 02:17:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Data Module"""
from __future__ import annotations

from typing import Dict

from pydantic.dataclasses import dataclass

from discover.asset.base.asset import AssetConfig
from discover.core.dstruct import DataClass
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.file.fileset import FileFormat


# ------------------------------------------------------------------------------------------------ #
#                                   FILESET CONFIG                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class FilesetConfig(DataClass):
    """Configures filesets, files, such as raw data files, that exist outside of the workspace"""

    filepath: str
    file_format: FileFormat
    dftype: DFType

    @classmethod
    def from_dict(cls, config: Dict[str, str]) -> FilesetConfig:
        try:
            filepath = config["filepath"]
            file_format = FileFormat.from_value(config["file_format"])
            dftype = DFType.from_value(config["dftype"])

            return cls(
                filepath=filepath,
                file_format=file_format,
                dftype=dftype,
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid dataset configuration: {e}")


# ------------------------------------------------------------------------------------------------ #
#                                   DATASET CONFIG                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetConfig(AssetConfig):
    """Configuration for datasets, inheriting from AssetConfig.

    Extends the base asset configuration by adding a specification
    for the dataset type.

    Args:
        dftype (DFType): The type of the dataset (e.g., structured, unstructured).

    Attributes:
        dftype (DFType): The type of the dataset (e.g., structured, unstructured).
    """

    dftype: DFType

    @classmethod
    def from_dict(cls, config: dict) -> "DatasetConfig":
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
            dftype = DFType.from_value(config["dftype"])
            file_format = FileFormat.from_value(config["file_format"])

            return cls(
                phase=phase,
                stage=stage,
                name=name,
                dftype=dftype,
                file_format=file_format,
            )
        except (KeyError, ValueError) as e:
            raise ValueError(f"Invalid dataset configuration: {e}")
