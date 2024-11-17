#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/datamanager/perplexity.py                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 15th 2024 12:07:41 am                                               #
# Modified   : Saturday November 16th 2024 06:31:03 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
from typing import Optional

import pandas as pd

from discover.infra.service.datamanager.base import DataManager
from discover.infra.utils.file.io import IOService


# ------------------------------------------------------------------------------------------------ #
class PerplexityAnalysisDataManager(DataManager):
    """Manages perplexity analysis data operations, extending the DataManager class.

    This class provides methods to handle perplexity data, including retrieval,
    checking for existence, and merging perplexity data with a main DataFrame.
    """

    def __init__(self) -> None:
        """Initializes the PerplexityAnalysisDataManager.

        Sets the file path for perplexity data based on the environment specified
        in the parent DataManager class.
        """
        super().__init__()
        self._perplexity_filepath = f"models/perplexity/perplexity_{self._env}.csv"

    def get_perplexity(self) -> Optional[pd.DataFrame]:
        """Retrieves the perplexity data from a file.

        Returns:
            Optional[pd.DataFrame]: The perplexity data as a DataFrame if the file exists, otherwise None.

        Raises:
            FileNotFoundError: If the perplexity file does not exist at the specified path.
        """
        try:
            return IOService.read(
                filepath=self._perplexity_filepath, lineterminator="\n"
            )
        except FileNotFoundError as e:
            msg = f"Perplexity file does not exist at {self._perplexity_filepath}\n{e}"
            raise FileNotFoundError(msg)

    def perplexity_exist(self) -> bool:
        """Checks if the perplexity data file exists at the specified path.

        Returns:
            bool: True if the perplexity file exists, False otherwise.
        """
        return os.path.exists(self._perplexity_filepath)

    def merge_perplexity(
        self, df: pd.DataFrame, perplexity: pd.DataFrame
    ) -> pd.DataFrame:
        """Merges the perplexity data with the main DataFrame on the "id" column.

        Args:
            df (pd.DataFrame): The main DataFrame to merge with perplexity data.
            perplexity (pd.DataFrame): The perplexity data to merge.

        Returns:
            pd.DataFrame: The merged DataFrame.

        Raises:
            ValueError: If the lengths of the main DataFrame and the perplexity DataFrame do not match.
        """
        if len(df) != len(perplexity):
            msg = (
                f"Incompatibility error: df has length of {len(df)} and perplexity have length "
                f"of {len(perplexity)}. Ensure that both dataframes were created in the same environment."
            )
            raise ValueError(msg)
        perplexity["id"] = perplexity["id"].astype("string")
        return df.merge(perplexity[["id", "an_perplexity"]], how="left", on="id")
