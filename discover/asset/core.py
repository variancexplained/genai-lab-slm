#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/core.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Friday December 27th 2024 04:59:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

from enum import Enum
from typing import Any


# ------------------------------------------------------------------------------------------------ #
#                                   ASSET TYPE                                                     #
# ------------------------------------------------------------------------------------------------ #
class AssetType(Enum):
    """
    Enum representing different types of assets in a machine learning workflow.

    Each asset type is associated with a string identifier (`value`) and a human-readable label (`label`).

    Attributes:
        DATASET: Represents a dataset asset.
        MODEL: Represents a model asset.
        EXPERIMENT: Represents an experiment asset.
        INFERENCE: Represents an inference asset.

    Args:
        value (str): The string identifier for the asset type.
        label (str): The human-readable label for the asset type.
    """

    DATASET = ("dataset", "Dataset")
    MODEL = ("model", "Model")
    EXPERIMENT = ("experiment", "Experiment")
    INFERENCE = ("inference", "Inference")

    def __new__(cls, value: str, label: str) -> Any:
        """
        Create a new instance of AssetType.

        Args:
            value (str): The string identifier for the asset type.
            label (str): The human-readable label for the asset type.

        Returns:
            AssetType: An instance of the AssetType enum.
        """
        obj = object.__new__(cls)
        obj._value_ = value  # Set the Enum value
        obj._label = label  # Set the custom label
        return obj

    @classmethod
    def from_value(cls, value) -> AssetType:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    @property
    def label(self) -> str:
        """
        Returns the human-readable label for the asset type.

        Returns:
            str: The label associated with the asset type.
        """
        return self._label
