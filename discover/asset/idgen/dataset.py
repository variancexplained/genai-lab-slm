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
# Modified   : Wednesday December 18th 2024 03:57:50 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from discover.asset.idgen.base import AssetIDGen
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
        phase: str,
        stage: str,
        name: str,
    ) -> str:
        """Generates a dataset asset ID for the data preparation phase.

        Args:
            phase (PhaseDef): The phase of data preparation (e.g., ingestion, cleaning).
            stage (StageDef): The stage within the phase (e.g., raw, processed).
            name (str): The name of the dataset.

        Returns:
            str: The generated dataset asset ID.
        """
        env = config_reader.get_environment()
        return f"dataset-{env}-{phase}-{stage}-{name}"
