#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/asset/model/identity.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:42:37 am                                               #
# Modified   : Sunday January 26th 2025 10:38:16 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Passport ID Generation Module"""
from __future__ import annotations

from genailab.asset.base.identity import AssetPassport
from pydantic.dataclasses import dataclass


# ------------------------------------------------------------------------------------------------ #
#                                      MODEL PASSPORT                                              #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ModelPassport(AssetPassport):
    """Encapsulates model identity metadata.

    This class provides a structured way to represent the identity models.

    Inherits from `AssetPassport`.
    """
