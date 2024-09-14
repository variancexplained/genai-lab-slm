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
# Modified   : Saturday September 14th 2024 03:50:56 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Ingest Task Module"""
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.service.base.task import Task
from discover.domain.service.data.ingest.config import IngestConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class IngestTask(Task):

    __STAGE = Stage.INGEST

    def __init__(self, config: IngestConfig, context: Context):
        """"""
        super().__init__(config=config, context=context)

    @property
    def stage(self) -> Stage:
        return self.__STAGE

    def run(self, data: Any):
        """Preprocess text data by ensuring string column, removing newlines, and verifying encoding."""
        super().run(data=data)

        data = self._remove_newlines(data=data)
        data = self._verify_encoding(data=data)
        data = self._cast_datatypes(data=data, datatypes=self._config.datatypes)
        data = self._trim_dataset(data=data)

        return data

    def _remove_newlines(self, data):
        """Remove newline characters from the specified column."""
        data[self._config.text_column] = data[self._config.text_column].str.replace(
            "\n", " "
        )
        self._logger.debug("Removed newlines")
        return data

    def _verify_encoding(self, data):
        """Verify and normalize the encoding of the specified column.

        A sample is checked for encoding errors. If encoding errors encountered,
        the entire data column is encoded. Otherwise, the encoding is skipped.
        """

        def check_sample_encoding(sample) -> bool:
            try:
                sample.parallel_apply(lambda x: x.encode("utf-8").decode("utf-8"))
                return False  # No encoding issues found
            except UnicodeEncodeError:
                return True  # Encoding issues found

        def re_encode_text(text):
            """Re-encode text to handle encoding issues."""
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
        """Casts columns to the designated data types"""

        self._logger.debug("Cast data types")
        for column, dtype in datatypes.items():
            if column in data.columns:
                data[column] = data[column].astype(dtype)
            else:
                msg = f"Column {column} not found in DataFrame"
                self._logger.exception(msg)
                raise ValueError(msg)

        return data

    def _trim_dataset(self, data: pd.DataFrame) -> None:
        """Drop shopping review"""
        # We only have about 9 reviews in this category.
        data = data.loc[data["category"] != "Shopping"]
        data["category"] = data["category"].cat.remove_unused_categories()
        self._logger.debug("Trimmed dataset of unused categories.")
        return data
