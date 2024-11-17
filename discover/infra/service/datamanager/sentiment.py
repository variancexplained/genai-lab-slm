#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/service/datamanager/sentiment.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday November 15th 2024 12:07:41 am                                               #
# Modified   : Saturday November 16th 2024 06:32:02 pm                                             #
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
class SentimentAnalysisDataManager(DataManager):
    """Manages sentiment analysis data operations, extending the DataManager class.

    This class provides additional functionality for handling sentiment data,
    including methods to retrieve and check for the existence of sentiment data,
    as well as merging sentiment data with a main DataFrame.
    """

    def __init__(self) -> None:
        """Initializes the SentimentAnalysisDataManager.

        Sets the file path for sentiment data based on the environment specified
        in the parent DataManager class.
        """
        super().__init__()
        self._sentiments_filepath = (
            f"models/sentiment/inference/sentiments_{self._env}.csv"
        )

    def get_sentiments(self) -> Optional[pd.DataFrame]:
        """Retrieves the sentiment data from a file.

        Returns:
            Optional[pd.DataFrame]: The sentiment data as a DataFrame if the file exists, otherwise None.

        Raises:
            FileNotFoundError: If the sentiment file does not exist at the specified path.
        """
        try:
            return IOService.read(
                filepath=self._sentiments_filepath, lineterminator="\n"
            )
        except FileNotFoundError as e:
            msg = f"Sentiment file does not exist at {self._sentiments_filepath}\n{e}"
            raise FileNotFoundError(msg)

    def sentiments_exist(self) -> bool:
        """Checks if the sentiment data file exists at the specified path.

        Returns:
            bool: True if the sentiment file exists, False otherwise.
        """
        return os.path.exists(self._sentiments_filepath)

    def merge_sentiments(
        self, df: pd.DataFrame, sentiments: pd.DataFrame
    ) -> pd.DataFrame:
        """Merges the sentiment data with the main DataFrame on the "id" column.

        Args:
            df (pd.DataFrame): The main DataFrame to merge with sentiment data.
            sentiments (pd.DataFrame): The sentiment data to merge.

        Returns:
            pd.DataFrame: The merged DataFrame.

        Raises:
            ValueError: If the lengths of the main DataFrame and the sentiments DataFrame do not match.
        """
        if len(df) != len(sentiments):
            msg = (
                f"Incompatibility error: df has length of {len(df)} and sentiments have length "
                f"of {len(sentiments)}. Ensure that both dataframes were created in the same environment."
            )
            raise ValueError(msg)
        sentiments["id"] = sentiments["id"].astype("string")
        return df.merge(sentiments[["id", "an_sentiment"]], how="left", on="id")
