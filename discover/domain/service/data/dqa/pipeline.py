#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/dqa/pipeline.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Monday September 16th 2024 02:22:54 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Assessment Module"""
from __future__ import annotations

import warnings
from typing import Any

import fasttext
import pandas as pd
from pandarallel import pandarallel

from discover.domain.base import Pipeline, PipelineBuilder
from discover.domain.service.core.cache import Cache
from discover.domain.service.data.dqa.task import (
    DetectDuplicateRowTask,
    DetectEmailTask,
    DetectEmojiTask,
    DetectInvalidDatesTask,
    DetectInvalidRatingsTask,
    DetectNonEnglishTask,
    DetectNullValuesTask,
    DetectOutliersTask,
    DetectPhoneNumberTask,
    DetectProfanityTask,
    DetectSpecialCharacterTask,
    DetectURLTask,
)
from discover.domain.value_objects.config import ServiceConfig
from discover.domain.value_objects.lifecycle import Stage

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
fasttext.FastText.eprint = lambda x: None


# ------------------------------------------------------------------------------------------------ #
#                                       DQA Pipeline                                               #
# ------------------------------------------------------------------------------------------------ #
class DQAPipeline(Pipeline):
    __STAGE = Stage.DQA

    def __init__(self, config: ServiceConfig, cache: Cache) -> None:
        super().__init__(config=config, stage=self.__STAGE)
        self._cache = cache
        self._reader = self._config.reader
        self._writer = self._config.writer

        self._data = None
        self._qa_data = pd.DataFrame()

    def _run_pipeline(self) -> Any:
        """Orchestrates the execution of the data quality pipeline"""

        self._data = self._reader.read()
        self._data = self._initialize(data=self._data)

        for task in self._tasks:
            # Check cache for task results.
            if not self._config.force and self._cache.exists(key=task.new_column_name):
                result = self._cache.get_item(key=task.new_column_name)
            else:
                result = task.run(self._data)
                self._cache.add_item(key=task.new_column_name, value=result)

            self._update_qa_data(result=result)

        self._data = self._finalize()
        self._writer.write(data=self._data)

    def _update_qa_data(self, result: pd.Series) -> None:
        """Updates qa_data"""
        # Convert series to DataFrame
        result_df = result.to_frame(name=self.new_column_name)
        # Append to qa_data
        self._qa_data = pd.concat([self._qa_data, result_df], axis=1)

    def _initialize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.reset_index().set_index(keys=[self._config.init_sort_by])
        data = data.sort_index(ascending=True)
        return data.reset_index()

    def _finalize(self) -> pd.DataFrame:
        return pd.concat([self._data, self._qa_data], axis=1)


# ------------------------------------------------------------------------------------------------ #
#                                    DQA Pipeline Builder                                          #
# ------------------------------------------------------------------------------------------------ #
class DQAPipelineBuilder(PipelineBuilder):
    """"""

    def __init__(
        self, config: ServiceConfig, pipeline_cls: type[Pipeline] = DQAPipeline
    ) -> None:
        """Initializes the DataQualityPipeline with data."""
        super().__init__(config=config, pipeline_cls=pipeline_cls)
        self._cache = self._config.cache_cls(name=self._config.cache_name)

    def create_pipeline(self) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = self._pipeline_cls(config=self._config)

        # Instantiate duplicate checkers
        dup1 = DetectDuplicateRowTask(new_column_name="is_duplicate", cache=self._cache)
        dup2 = DetectDuplicateRowTask(
            column_names="id",
            new_column_name="is_duplicate_review_id",
            cache=self._cache,
        )
        # Instantiate detection of null values
        null = DetectNullValuesTask(
            new_column_name="has_null_values", cache=self._cache
        )

        # Instantiate detection of outliers
        out_vote_sum = DetectOutliersTask(
            column_name="vote_sum",
            new_column_name="vote_sum_outlier",
            cache=self._cache,
        )
        out_vote_count = DetectOutliersTask(
            column_name="vote_count",
            new_column_name="vote_count_outlier",
            cache=self._cache,
        )
        # Instantiate detection of non-english text in reviews and app names.
        lang_review = DetectNonEnglishTask(
            text_column="content",
            new_column_name="has_non_english_review",
            n_jobs=self.config.n_jobs,
            cache=self._cache,
        )
        lang_app_name = DetectNonEnglishTask(
            text_column="app_name",
            new_column_name="has_non_english_app_name",
            n_jobs=self.config.n_jobs,
            cache=self._cache,
        )
        # Instantiate detection of emojis
        emoji = DetectEmojiTask(new_column_name="has_emojis", cache=self._cache)
        # Instantiate detection of excessive special characters
        chars = DetectSpecialCharacterTask(
            threshold=0.3,
            new_column_name="has_excessive_special_chars",
            cache=self._cache,
        )
        # Instantiate date validator
        dates = DetectInvalidDatesTask(
            new_column_name="has_invalid_date", cache=self._cache
        )
        # Instantiate a rating validator
        ratings = DetectInvalidRatingsTask(
            new_column_name="has_invalid_rating", cache=self._cache
        )
        # Instantiate a profanity check
        profanity = DetectProfanityTask(
            n_jobs=self.config.n_jobs,
            new_column_name="has_profanity",
            cache=self._cache,
        )
        # Instantiate email, URL, and phone number detection in review text.
        emails = DetectEmailTask(new_column_name="contains_email", cache=self._cache)
        urls = DetectURLTask(new_column_name="contains_url", cache=self._cache)
        phones = DetectPhoneNumberTask(
            new_column_name="contains_phone_number", cache=self._cache
        )

        # Add tasks to pipeline, starting with the lower resource intensive tasks.
        pipe.add_task(dup1)
        pipe.add_task(dup2)
        pipe.add_task(null)
        pipe.add_task(out_vote_sum)
        pipe.add_task(out_vote_count)
        pipe.add_task(emoji)
        pipe.add_task(chars)
        pipe.add_task(dates)
        pipe.add_task(ratings)
        pipe.add_task(emails)
        pipe.add_task(urls)
        pipe.add_task(phones)
        # Add resource moderate tasks.
        pipe.add_task(lang_review)
        pipe.add_task(lang_app_name)
        # Add resource intensive task
        pipe.add_task(profanity)
        return pipe
