#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/assets/idgen.py                                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 01:36:41 am                                              #
# Modified   : Monday October 21st 2024 07:18:59 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
config_reader = AppConfigReader()


# ------------------------------------------------------------------------------------------------ #
class AssetIDGen:
    """
    A utility class for generating asset IDs based on asset type, phase, stage, and name.
    Provides a static method for constructing a standardized asset ID string.
    """

    @staticmethod
    def get_asset_id(
        asset_type: str,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
    ) -> str:
        """
        Generates an asset ID string based on the provided asset type, phase, stage, and name.

        Args:
            asset_type (str): The type of the asset (e.g., 'dataset', 'model').
            phase (PhaseDef): The phase definition object representing the phase of the asset.
            stage (StageDef): The stage definition object representing the stage of the asset.
            name (str): The specific name or identifier for the asset.

        Returns:
            str: A standardized asset ID string in the format:
                 "{asset_type}-{phase.value}-{stage.value}-{name}"
        """
        env = config_reader.get_environment()
        return f"{asset_type}-{env}-{phase.value}-{stage.value}-{name}"
