#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/workspace/idgen.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 02:42:37 am                                               #
# Modified   : Sunday January 5th 2025 12:05:51 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""ID Generation Module"""

from discover.asset.base.atype import AssetType
from discover.asset.base.identity import AssetId
from discover.core.flow import PhaseDef, StageDef
from discover.infra.workspace.version import VersionManager

# ------------------------------------------------------------------------------------------------ #


class IDGen:
    def __init__(self, version_manager: VersionManager) -> None:
        """
        Initializes the ID generator with a version manager.

        Args:
            version_manager (VersionManager): The version manager for generating sequence numbers.
        """
        self._version_manager = version_manager

    def gen_asset_id(
        self, asset_type: AssetType, phase: PhaseDef, stage: StageDef, name: str
    ) -> str:
        """
        Generates a unique asset ID based on the asset type, phase, stage, and name.

        Args:
            asset_type (AssetType): The type of asset, with a 'value' attribute.
            phase (PhaseDef): The phase definition, with an 'id' attribute.
            stage (StageDef): The stage definition, with a 'directory' attribute.
            name (str): The name of the asset.

        Returns:
            str: A unique asset ID in the format "{asset_type}_{name}_{version}".
        """
        if not all([asset_type, phase, stage, name]):
            raise ValueError("All arguments must be provided and non-empty.")

        version = self._version_manager.get_next_version(phase=phase, stage=stage)
        name = f"{asset_type.value}_{phase.value}_{stage.value}_{name}_{version}"
        return AssetId(name=name, version=version)
