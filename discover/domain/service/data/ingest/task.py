#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/ingest/task.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Saturday September 14th 2024 06:16:12 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Task Module"""
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.base.task import Task
from discover.domain.service.data.ingest.config import IngestConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.monitor.profiler import profiler

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class IngestTask(Task):
    """
    Task for preprocessing data during the ingestion stage.

    This task handles text data preprocessing, including removing newlines,
    verifying encoding, casting data types, and trimming the dataset. It is
    part of the data ingestion pipeline and ensures the data is cleaned and
    properly formatted for further processing.

    Attributes:
    -----------
    __STAGE : Stage
        The stage of the task, which is set to INGEST.
    _config : IngestConfig
        Configuration object containing settings such as data columns and datatypes.
    _context : Context
        Context object tracking metadata related to task execution.

    Methods:
    --------
    run(data: Any) -> Any:
        Executes the task by preprocessing the input data, including removing newlines,
        verifying encoding, casting datatypes, and trimming the dataset.

    _remove_newlines(data: pd.DataFrame) -> pd.DataFrame:
        Removes newline characters from the specified text column.

    _verify_encoding(data: pd.DataFrame) -> pd.DataFrame:
        Verifies the encoding of the text column and re-encodes if necessary.

    _cast_datatypes(data: pd.DataFrame, datatypes: Dict[str, type]) -> pd.DataFrame:
        Casts columns to the designated data types as specified in the configuration.

    _trim_dataset(data: pd.DataFrame) -> pd.DataFrame:
        Removes entries in the "Shopping" category and drops unused categories.
    """

    __STAGE = Stage.INGEST

    def __init__(self, config: IngestConfig, context: Context):
        """
        Initializes the IngestTask with the given configuration and context.

        Parameters:
        -----------
        config : IngestConfig
            Configuration object containing settings such as data columns and datatypes.
        context : Context
            Context object used to track task execution metadata such as stage and service type.
        """
        super().__init__(config=config, context=context)

    @property
    def stage(self) -> Stage:
        """
        Returns the current stage of the task, which is set to INGEST.

        Returns:
        --------
        Stage:
            The stage of the task (INGEST).
        """
        return self.__STAGE

    @profiler
    def run(self, data: Any):
        """
        Executes the preprocessing of text data by removing newlines, verifying encoding,
        casting datatypes, and trimming the dataset.

        Parameters:
        -----------
        data : Any
            The input data to be preprocessed.

        Returns:
        --------
        Any:
            The preprocessed data after performing all transformations.
        """
        super().run(data=data)

        data = self._remove_newlines(data=data)
        data = self._verify_encoding(data=data)
        data = self._cast_datatypes(data=data, datatypes=self._config.datatypes)
        data = self._trim_dataset(data=data)

        return data

    def _remove_newlines(self, data):
        """
        Removes newline characters from the specified text column.

        Parameters:
        -----------
        data : pd.DataFrame
            The input data containing the text column.

        Returns:
        --------
        pd.DataFrame:
            The data with newlines removed from the text column.
        """
        data[self._config.text_column] = data[self._config.text_column].str.replace(
            "\n", " "
        )
        self._logger.debug("Removed newlines")
        return data

    def _verify_encoding(self, data):
        """
        Verifies and normalizes the encoding of the specified text column.

        A sample of the data is checked for encoding issues. If encoding issues are
        detected, the entire column is re-encoded. Otherwise, the encoding is skipped.

        Parameters:
        -----------
        data : pd.DataFrame
            The input data containing the text column.

        Returns:
        --------
        pd.DataFrame:
            The data with normalized encoding in the text column.
        """

        def check_sample_encoding(sample) -> bool:
            try:
                sample.parallel_apply(lambda x: x.encode("utf-8").decode("utf-8"))
                return False  # No encoding issues found
            except UnicodeEncodeError:
                return True  # Encoding issues found

        def re_encode_text(text):
            """Re-encodes text to handle encoding issues."""
            try:
                return text.encode("utf-8").decode("utf-8")
            except UnicodeEncodeError:
                self._logger.debug(f"Encoding issue found in text: {text}")
                return text.encode("utf-8", errors="ignore").decode("utf-8")

        sample = data[self._config.text_column].sample(
            frac=self._config.encoding_sample, random_state=self._config.random_state
        )
        if check_sample_encoding(sample=sample):
            self._logger.debug(
                "Encoding issues found in sample. Re-encoding the entire column."
            )
            data[self._config.text_column] = data[
                self._config.text_column
            ].parallel_apply(re_encode_text)
        else:
            self._logger.debug(
                "No encoding issues found in sample. Skipping re-encoding."
            )
        return data

    def _cast_datatypes(
        self, data: pd.DataFrame, datatypes: Dict[str, type]
    ) -> pd.DataFrame:
        """
        Casts columns to the designated data types as specified in the configuration.

        Parameters:
        -----------
        data : pd.DataFrame
            The input data containing columns to be cast.
        datatypes : dict
            A dictionary mapping column names to their expected data types.

        Returns:
        --------
        pd.DataFrame:
            The data with columns cast to the specified data types.
        """
        self._logger.debug("Cast data types")
        for column, dtype in datatypes.items():
            if column in data.columns:
                data[column] = data[column].astype(dtype)
            else:
                msg = f"Column {column} not found in DataFrame"
                self._logger.exception(msg)
                raise ValueError(msg)

        return data

    def _trim_dataset(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Removes the "Shopping" category from the dataset and drops unused categories.

        Parameters:
        -----------
        data : pd.DataFrame
            The input data containing a "category" column.

        Returns:
        --------
        pd.DataFrame:
            The trimmed dataset with the "Shopping" category removed.
        """
        # We only have about 9 reviews in this category.
        data = data.loc[data["category"] != "Shopping"]
        data["category"] = data["category"].cat.remove_unused_categories()
        self._logger.debug("Trimmed dataset of unused categories.")
        return data
