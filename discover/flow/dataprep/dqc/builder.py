#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/dqc/builder.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Friday January 3rd 2025 02:20:54 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from typing import Dict

from discover.asset.dataset.dataset import Dataset
from discover.flow.base.builder import StageBuilder
from discover.flow.base.stage import Stage


# ------------------------------------------------------------------------------------------------ #
class DataQualityCheckStageBuilder(StageBuilder):
    def __init__(self) -> None:
        """
        Initializes the DataQualityCheckStageBuilder with default settings and task configuration.

        Args:
            None
        """
        super().__init__()

        self._detect_accents = None
        self._detect_control_chars = None
        self._detect_duplicate_review_ids = None
        self._detect_duplicate_reviews = None
        self._detect_duplicate_rows = None
        self._detect_elongation = None
        self._detect_emails = None
        self._detect_excess_special_chars = None
        self._detect_excess_whitespace = None
        self._detect_html = None
        self._detect_invalid_categories = None
        self._detect_invalid_ratings = None
        self._detect_invalid_review_dates = None
        self._detect_less_than_threshold = None
        self._detect_non_english_app_names = None
        self._detect_non_english_reviews = None
        self._detect_phone_numbers = None
        self._detect_repeated_chars = None
        self._detect_repeated_phrases = None
        self._detect_repeated_sequences = None
        self._detect_repeated_words = None
        self._detect_short_reviews = None
        self._detect_urls = None

        self._config = self._get_task_config()

        self._spark = None

    def reset(self) -> None:
        super().reset()
        self._detect_accents = None
        self._detect_control_chars = None
        self._detect_duplicate_review_ids = None
        self._detect_duplicate_reviews = None
        self._detect_duplicate_rows = None
        self._detect_elongation = None
        self._detect_emails = None
        self._detect_excess_special_chars = None
        self._detect_excess_whitespace = None
        self._detect_html = None
        self._detect_invalid_categories = None
        self._detect_invalid_ratings = None
        self._detect_invalid_review_dates = None
        self._detect_less_than_threshold = None
        self._detect_non_english_app_names = None
        self._detect_non_english_reviews = None
        self._detect_phone_numbers = None
        self._detect_repeated_chars = None
        self._detect_repeated_phrases = None
        self._detect_repeated_sequences = None
        self._detect_repeated_words = None
        self._detect_short_reviews = None
        self._detect_urls = None

    def detect_accents(self) -> DataQualityCheckStageBuilder:
        self._detect_accents = self._config["detect_accents"]
        self._tasks.append(self._task_builder.build(self._detect_accents))
        return self

    def detect_control_chars(self) -> DataQualityCheckStageBuilder:
        self._detect_control_chars = self._config["detect_control_chars"]
        self._tasks.append(self._task_builder.build(self._detect_control_chars))

        return self

    def detect_duplicate_review_ids(self) -> DataQualityCheckStageBuilder:
        self._detect_duplicate_review_ids = self._config["detect_duplicate_review_ids"]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_review_ids))

        return self

    def detect_duplicate_reviews(self) -> DataQualityCheckStageBuilder:
        self._detect_duplicate_reviews = self._config["detect_duplicate_reviews"]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_reviews))

        return self

    def detect_duplicate_rows(self) -> DataQualityCheckStageBuilder:
        self._detect_duplicate_rows = self._config["detect_duplicate_rows"]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_rows))

        return self

    def detect_elongation(
        self, threshold: int = 4, max_elongation: int = 3
    ) -> DataQualityCheckStageBuilder:
        self._detect_elongation = self._config["detect_elongation"]
        self._detect_elongation["params"]["threshold"] = threshold
        self._detect_elongation["params"]["max_elongation"] = max_elongation
        self._tasks.append(self._detect_elongation)
        return self

    def detect_emails(self) -> DataQualityCheckStageBuilder:
        self._detect_emails = self._config["detect_emails"]
        self._tasks.append(self._task_builder.build(self._detect_emails))

        return self

    def detect_excess_special_chars(
        self,
        threshold: float = 0.35,
        threshold_type: str = "proportion",
        unit: str = "character",
    ) -> DataQualityCheckStageBuilder:
        self._detect_excess_special_chars = self._config["detect_excess_special_chars"]
        self._detect_excess_special_chars["params"]["threshold"] = threshold
        self._detect_excess_special_chars["params"]["threshold_type"] = threshold_type
        self._detect_excess_special_chars["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._detect_excess_special_chars))
        return self

    def detect_excess_whitespace(self) -> DataQualityCheckStageBuilder:
        self._detect_excess_whitespace = self._config["detect_excess_whitespace"]
        self._tasks.append(self._task_builder.build(self._detect_excess_whitespace))
        return self

    def detect_html(self) -> DataQualityCheckStageBuilder:
        self._detect_html = self._config["detect_html"]
        self._tasks.append(self._task_builder.build(self._detect_html))

        return self

    def detect_invalid_categories(self) -> DataQualityCheckStageBuilder:
        self._detect_invalid_categories = self._config["detect_invalid_categories"]
        self._tasks.append(self._task_builder.build(self._detect_invalid_categories))

        return self

    def detect_invalid_ratings(self) -> DataQualityCheckStageBuilder:
        self._detect_invalid_ratings = self._config["detect_invalid_ratings"]
        self._tasks.append(self._task_builder.build(self._detect_invalid_ratings))

        return self

    def detect_invalid_review_dates(
        self, range_min: int = 2020, range_max: int = 2024, range_type: str = "year"
    ) -> DataQualityCheckStageBuilder:
        self._detect_invalid_review_dates = self._config["detect_invalid_review_dates"]
        self._detect_invalid_review_dates["params"]["ramge_min"] = range_min
        self._detect_invalid_review_dates["params"]["ramge_max"] = range_max
        self._detect_invalid_review_dates["params"]["ramge_type"] = range_type
        self._tasks.append(self._task_builder.build(self._detect_invalid_review_dates))

        return self

    def detect_non_english_app_names(self) -> DataQualityCheckStageBuilder:
        self._detect_non_english_app_names = self._config[
            "detect_non_english_app_names"
        ]
        self._tasks.append(self._task_builder.build(self._detect_non_english_app_names))

        return self

    def detect_non_english_reviews(self) -> DataQualityCheckStageBuilder:
        self._detect_non_english_reviews = self._config["detect_non_english_reviews"]
        self._tasks.append(self._task_builder.build(self._detect_non_english_reviews))

        return self

    def detect_phone_numbers(self) -> DataQualityCheckStageBuilder:
        self._detect_phone_numbers = self._config["detect_phone_numbers"]
        self._tasks.append(self._task_builder.build(self._detect_phone_numbers))
        return self

    def detect_repeated_chars(
        self, min_repetitions: int = 4
    ) -> DataQualityCheckStageBuilder:
        self._detect_repeated_chars = self._config["detect_repeated_chars"]
        self._detect_repeated_chars["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_chars))

        return self

    def detect_repeated_phrases(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 2,
    ) -> DataQualityCheckStageBuilder:
        self._detect_repeated_phrases = self._config["detect_repeated_phrases"]
        self._detect_repeated_phrases["params"]["threshold"] = threshold
        self._detect_repeated_phrases["params"]["threshold_type"] = threshold_type
        self._detect_repeated_phrases["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_phrases))

        return self

    def detect_repeated_sequences(
        self,
        length_of_sequence: int = 3,
        min_repetitions: int = 3,
        threshold: int = 3,
        threshold_type: str = "count",
        unit: str = "character",
    ) -> DataQualityCheckStageBuilder:
        self._detect_repeated_sequences = self._config["detect_repeated_sequences"]
        self._detect_repeated_sequences["params"][
            "length_of_sequence"
        ] = length_of_sequence
        self._detect_repeated_sequences["params"]["min_repetitions"] = min_repetitions
        self._detect_repeated_sequences["params"]["threshold"] = threshold
        self._detect_repeated_sequences["params"]["threshold_type"] = threshold_type
        self._detect_repeated_sequences["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._detect_repeated_sequences))

        return self

    def detect_repeated_words(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 3,
    ) -> DataQualityCheckStageBuilder:
        self._detect_repeated_words = self._config["detect_repeated_words"]
        self._detect_repeated_words["params"]["threshold"] = threshold
        self._detect_repeated_words["params"]["threshold_type"] = threshold_type
        self._detect_repeated_words["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_words))

        return self

    def detect_short_reviews(self, threshold: int = 3) -> DataQualityCheckStageBuilder:
        self._detect_short_reviews = self._config["detect_short_reviews"]
        self._detect_short_reviews["params"]["threshold"] = threshold
        self._tasks.append(self._task_builder.build(self._detect_short_reviews))
        return self

    def detect_urls(self) -> DataQualityCheckStageBuilder:
        self._detect_urls = self._config["detect_urls"]
        self._tasks.append(self._task_builder.build(self._detect_urls))

        return self

    def build(self) -> DataQualityCheckStageBuilder:
        """
        Builds the Ingest stage by validating configurations, constructing datasets,
        and assembling tasks.

        Returns:
            DataQualityCheckStageBuilder: The builder instance with the constructed stage.
        """
        self._validate()
        self._source = self._build_source_dataset()
        self._target = self._build_target_dataset()
        self._stage = Stage(
            source=self._source,
            target=self._target,
            tasks=self._tasks,
            state=self._state,
            repo=self._repo,
            spark=self._spark,
        )
        return self

    def _build_source_dataset(self) -> Dataset:
        """
        Builds the source dataset from the provided passport and filepath.

        Returns:
            Dataset: The constructed source dataset.
        """
        self._spark = self._spark or self._spark_session_pool.spark
        return self._repo.get(
            asset_id=self._source_passport.asset_id,
            dftype=self._source_passport.dftype,
            spark=self._spark,
        )

    def _build_target_dataset(self) -> Dataset:
        """
        Builds the target dataset from the provided passport.

        Returns:
            Dataset: The constructed target dataset.
        """
        dataset = self._dataset_builder.passport(self._target_passport).build().dataset
        return dataset

    def _validate(self) -> None:
        """
        Validates the configurations and settings for the Ingest stage.

        Ensures that required fields such as the source filepath, encoding, datatypes,
        and datetime conversion tasks are defined.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if self._detect_control_chars is None:
            errors.append(
                "detect_control_chars is a required step in the DataQualityCheck Stage."
            )
        if self._detect_duplicate_review_ids is None:
            errors.append(
                "detect_duplicate_review_ids is a required step in the DataQualityCheck Stage."
            )
        if self._detect_duplicate_reviews is None:
            errors.append(
                "detect_duplicate_reviews is a required step in the DataQualityCheck Stage."
            )
        if self._detect_duplicate_rows is None:
            errors.append(
                "detect_duplicate_rows is a required step in the DataQualityCheck Stage."
            )
        if self._detect_elongation is None:
            errors.append(
                "detect_elongation is a required step in the DataQualityCheck Stage."
            )
        if self._detect_emails is None:
            errors.append(
                "detect_emails is a required step in the DataQualityCheck Stage."
            )
        if self._detect_excess_whitespace is None:
            errors.append(
                "detect_excess_whitespace is a required step in the DataQualityCheck Stage."
            )
        if self._detect_html is None:
            errors.append(
                "detect_html is a required step in the DataQualityCheck Stage."
            )
        if self._detect_invalid_categories is None:
            errors.append(
                "detect_invalid_categories is a required step in the DataQualityCheck Stage."
            )
        if self._detect_invalid_ratings is None:
            errors.append(
                "detect_invalid_ratings is a required step in the DataQualityCheck Stage."
            )
        if self._detect_invalid_review_dates is None:
            errors.append(
                "detect_invalid_review_dates is a required step in the DataQualityCheck Stage."
            )
        if self._detect_phone_numbers is None:
            errors.append(
                "detect_phone_numbers is a required step in the DataQualityCheck Stage."
            )
        if self._detect_urls is None:
            errors.append(
                "detect_urls is a required step in the DataQualityCheck Stage."
            )

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)

    def _get_task_config(self) -> Dict[str, Dict[str, str]]:
        """
        Retrieves the configuration for tasks in the Ingest stage from the config reader.

        Returns:
            Dict[str, Dict[str, str]]: The task configuration dictionary.
        """
        try:
            return self._config_reader.get_config(section="phases", namespace=False)[
                "dataprep"
            ]["stages"]["dqc"]["tasks"]
        except KeyError as e:
            msg = f"Configuration Error. Unable to obtain task configurations from config. Check your config.yaml file.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
        except Exception as e:
            msg = f"Unrecognized error occurred while accessing the task configuration.\n{e}"
            self._logger.error(msg)
            raise RuntimeError(msg)
