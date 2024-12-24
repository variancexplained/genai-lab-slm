#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/idgen/dataset.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 01:36:41 am                                              #
# Modified   : Monday December 23rd 2024 12:03:20 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from discover.asset.idgen.base import AssetIDGen
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader

# ------------------------------------------------------------------------------------------------ #
config_reader = AppConfigReader()


# ------------------------------------------------------------------------------------------------ #
class DatasetIDGen(AssetIDGen):
    """Concrete implementation of AssetIDGen for dataset preparation.

    Generates asset IDs for datasets created during the data preparation phase.

    Methods:
        generate_asset_id: Generates asset IDs based on the data preparation phase, stage, and dataset name.
    """

    @staticmethod
    def generate_asset_id(
        phase: PhaseDef,
        stage: StageDef,
        name: str,
    ) -> str:
        """Generates a dataset asset ID for the data preparation phase.

        Args:
            phase (str): A project phase, i.e. data_prep, or 'absa_pretrainiing'.
            stage (str): A stage or experiment.
            name (str): The name of the dataset.

        Returns:
            str: The generated dataset asset ID.
        """
        name = name.replace(" ", "_").lower()
        env = config_reader.get_environment().lower()
        return f"dataset-{env}-{phase.value}-{stage.value}-{name}"
