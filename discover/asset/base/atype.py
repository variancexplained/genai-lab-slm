#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/base/atype.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 29th 2024 01:54:33 pm                                               #
# Modified   : Tuesday December 31st 2024 11:46:25 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Asset Type Module"""
from __future__ import annotations

from enum import Enum

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

    def __new__(cls, value: str, label: str):
        obj = object.__new__(cls)
        obj._value_ = value
        obj._label = label
        return obj

    @property
    def label(self):
        """Get the human-readable label for the asset type."""
        return self._label

    @classmethod
    def from_value(cls, value: str) -> "AssetType":
        """Class method to retrieve an AssetType from its string value."""
        for asset_type in cls:
            if asset_type.value == value:
                return asset_type
        raise ValueError(f"{value} is not a valid AssetType")
