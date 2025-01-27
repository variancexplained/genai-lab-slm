#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/asset/base/component.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 03:01:02 pm                                            #
# Modified   : Saturday January 25th 2025 04:40:45 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Base Module for the Asset Dimension"""
from __future__ import annotations

from genailab.core.dstruct import DataClass
from pydantic.dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                    ASSET COMPONENT                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass(config=dict(arbitrary_types_allowed=True))
class AssetComponent(DataClass):
    """Base class for asset component subclasses."""
