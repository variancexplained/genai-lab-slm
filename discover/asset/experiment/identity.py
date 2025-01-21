#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/experiment/identity.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:42:37 am                                               #
# Modified   : Tuesday January 21st 2025 06:28:33 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from pydantic.dataclasses import dataclass

from discover.asset.base.identity import AssetPassport


# ------------------------------------------------------------------------------------------------ #
#                                      EXPERIMENT PASSPORT                                         #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ExperimentPassport(AssetPassport):
    """Encapsulates experiment identity metadata.

    This class provides a structured way to represent the identity experiments.

    Inherits from `AssetPassport`.
    """
