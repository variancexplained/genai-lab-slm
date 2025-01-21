#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/passport/idgen.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 21st 2025 03:21:59 am                                               #
# Modified   : Tuesday January 21st 2025 06:08:09 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Workspace Identity Base Module"""
from __future__ import annotations

from abc import ABC

from discover.core.flow import PhaseDef, StageDef


# ------------------------------------------------------------------------------------------------ #
#                                          ID GENERATOR                                            #
# ------------------------------------------------------------------------------------------------ #
class IDGen(ABC):
    """Base class that defines asset_id assignment for all assets in the workspace."""

    @staticmethod
    def gen_asset_id(
        asset_type: str, phase: PhaseDef, stage: StageDef, name: str, version: str
    ) -> str:
        """Returns an asset id for a given asset"""
        return f"{phase.value}_{stage.value}_{asset_type}_{name}_{version}"
