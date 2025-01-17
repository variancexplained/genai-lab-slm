#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/flow/dataprep/dqa/builder.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Thursday January 16th 2025 07:31:51 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from discover.core.flow import PhaseDef, StageDef
from discover.flow.base.builder import StageBuilder
from discover.flow.dataprep.dqa.stage import DataQualityAssessmentStage


# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentStageBuilder(StageBuilder):
    """Builder class for the Data Quality Assessment (DQA) stage in a data processing pipeline.

    This class extends the `StageBuilder` to specifically handle the construction
    of a data quality check stage, which includes a variety of data validation and
    quality assurance tasks. Each detection method corresponds to a specific quality
    check, which can be configured and added to the stage.

    Attributes:
        __PHASE (PhaseDef): The phase definition for Data Preparation.
        __STAGE (StageDef): The stage definition for Data Quality Assessment.
        _detect_accents (Optional[dict]): Configuration for accent detection.
        _detect_control_chars (Optional[dict]): Configuration for control character detection.
        _detect_duplicate_review_ids (Optional[dict]): Configuration for duplicate review ID detection.
        _detect_duplicate_reviews (Optional[dict]): Configuration for duplicate review detection.
        _detect_duplicate_rows (Optional[dict]): Configuration for duplicate row detection.
        _detect_elongation (Optional[dict]): Configuration for elongation detection.
        _detect_emails (Optional[dict]): Configuration for email detection.
        _detect_excess_special_chars (Optional[dict]): Configuration for excess special character detection.
        _detect_excess_whitespace (Optional[dict]): Configuration for excess whitespace detection.
        _detect_html (Optional[dict]): Configuration for HTML content detection.
        _detect_invalid_categories (Optional[dict]): Configuration for invalid category detection.
        _detect_invalid_ratings (Optional[dict]): Configuration for invalid ratings detection.
        _detect_invalid_review_dates (Optional[dict]): Configuration for invalid review date detection.
        _detect_less_than_threshold (Optional[dict]): Configuration for threshold-based detection.
        _detect_non_english_app_names (Optional[dict]): Configuration for non-English app name detection.
        _detect_non_english_reviews (Optional[dict]): Configuration for non-English review detection.
        _detect_phone_numbers (Optional[dict]): Configuration for phone number detection.
        _detect_repeated_chars (Optional[dict]): Configuration for repeated character detection.
        _detect_repeated_phrases (Optional[dict]): Configuration for repeated phrase detection.
        _detect_repeated_sequences (Optional[dict]): Configuration for repeated sequence detection.
        _detect_repeated_words (Optional[dict]): Configuration for repeated word detection.
        _detect_short_reviews (Optional[dict]): Configuration for short review detection.
        _detect_urls (Optional[dict]): Configuration for URL detection.
        _config (dict): Configuration for the DQA stage tasks.
        _spark (Optional[Any]): Spark session, initialized as None.
        _tasks (List[Any]): List of tasks built by this stage.
        _logger (logging.Logger): Logger instance for this class.

    Args:
        None
    """

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.DQA

    def __init__(self) -> None:
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

        self._task_configs = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )
        self._source_config = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="source_config"
        )

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

        self._task_configs = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )

    # -------------------------------------------------------------------------------------------- #
    def detect_privacy_issues(self) -> DataQualityAssessmentStageBuilder:
        self.detect_emails()
        self.detect_phone_numbers()
        self.detect_urls()
        return self

    def detect_urls(self) -> None:
        self._detect_urls = self._task_configs["detect_urls"]
        self._tasks.append(self._task_builder.build(self._detect_urls))

    def detect_phone_numbers(self) -> None:
        self._detect_phone_numbers = self._task_configs["detect_phone_numbers"]
        self._tasks.append(self._task_builder.build(self._detect_phone_numbers))

    def detect_emails(self) -> None:
        self._detect_emails = self._task_configs["detect_emails"]
        self._tasks.append(self._task_builder.build(self._detect_emails))

    # -------------------------------------------------------------------------------------------- #
    def detect_duplication(self) -> DataQualityAssessmentStageBuilder:
        self.detect_duplicate_review_ids()
        self.detect_duplicate_reviews()
        self.detect_duplicate_rows()
        return self

    def detect_duplicate_review_ids(self) -> None:
        self._detect_duplicate_review_ids = self._task_configs[
            "detect_duplicate_review_ids"
        ]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_review_ids))

    def detect_duplicate_reviews(self) -> None:
        self._detect_duplicate_reviews = self._task_configs["detect_duplicate_reviews"]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_reviews))

    def detect_duplicate_rows(self) -> None:
        self._detect_duplicate_rows = self._task_configs["detect_duplicate_rows"]
        self._tasks.append(self._task_builder.build(self._detect_duplicate_rows))

    # -------------------------------------------------------------------------------------------- #
    def detect_invalid_characters(self) -> DataQualityAssessmentStageBuilder:
        self.detect_accents()
        self.detect_control_chars()
        self.detect_html()
        return self

    def detect_accents(self) -> None:
        self._detect_accents = self._task_configs["detect_accents"]
        self._tasks.append(self._task_builder.build(self._detect_accents))

    def detect_control_chars(self) -> None:
        self._detect_control_chars = self._task_configs["detect_control_chars"]
        self._tasks.append(self._task_builder.build(self._detect_control_chars))

    def detect_html(self) -> None:
        self._detect_html = self._task_configs["detect_html"]
        self._tasks.append(self._task_builder.build(self._detect_html))

    # -------------------------------------------------------------------------------------------- #
    def detect_invalid_values(self) -> DataQualityAssessmentStageBuilder:
        self.detect_invalid_categories()
        self.detect_invalid_ratings()
        self.detect_invalid_review_dates()
        return self

    def detect_invalid_categories(self) -> None:
        self._detect_invalid_categories = self._task_configs[
            "detect_invalid_categories"
        ]
        self._tasks.append(self._task_builder.build(self._detect_invalid_categories))

    def detect_invalid_ratings(self) -> None:
        self._detect_invalid_ratings = self._task_configs["detect_invalid_ratings"]
        self._tasks.append(self._task_builder.build(self._detect_invalid_ratings))

    def detect_invalid_review_dates(
        self, range_min: int = 2008, range_max: int = 2024, range_type: str = "year"
    ) -> None:
        self._detect_invalid_review_dates = self._task_configs[
            "detect_invalid_review_dates"
        ]
        self._detect_invalid_review_dates["params"]["ramge_min"] = range_min
        self._detect_invalid_review_dates["params"]["ramge_max"] = range_max
        self._detect_invalid_review_dates["params"]["ramge_type"] = range_type
        self._tasks.append(self._task_builder.build(self._detect_invalid_review_dates))

    # -------------------------------------------------------------------------------------------- #
    def detect_non_english(self) -> DataQualityAssessmentStageBuilder:
        self.detect_non_english_app_names()
        self.detect_non_english_reviews()
        return self

    def detect_non_english_app_names(self) -> None:
        self._detect_non_english_app_names = self._task_configs[
            "detect_non_english_app_names"
        ]
        self._tasks.append(self._task_builder.build(self._detect_non_english_app_names))

    def detect_non_english_reviews(self) -> None:
        self._detect_non_english_reviews = self._task_configs[
            "detect_non_english_reviews"
        ]
        self._tasks.append(self._task_builder.build(self._detect_non_english_reviews))

    # -------------------------------------------------------------------------------------------- #
    def detect_elongation(
        self, threshold: int = 4, max_elongation: int = 3
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_elongation = self._task_configs["detect_elongation"]
        self._detect_elongation["params"]["threshold"] = threshold
        self._detect_elongation["params"]["max_elongation"] = max_elongation
        self._tasks.append(self._task_builder.build(self._detect_elongation))
        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_excess_special_chars(
        self,
        threshold: float = 0.35,
        threshold_type: str = "proportion",
        unit: str = "character",
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_excess_special_chars = self._task_configs[
            "detect_excess_special_chars"
        ]
        self._detect_excess_special_chars["params"]["threshold"] = threshold
        self._detect_excess_special_chars["params"]["threshold_type"] = threshold_type
        self._detect_excess_special_chars["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._detect_excess_special_chars))
        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_excess_whitespace(self) -> DataQualityAssessmentStageBuilder:
        self._detect_excess_whitespace = self._task_configs["detect_excess_whitespace"]
        self._tasks.append(self._task_builder.build(self._detect_excess_whitespace))
        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_chars(
        self, min_repetitions: int = 4
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_chars = self._task_configs["detect_repeated_chars"]
        self._detect_repeated_chars["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_chars))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_phrases(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 2,
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_phrases = self._task_configs["detect_repeated_phrases"]
        self._detect_repeated_phrases["params"]["threshold"] = threshold
        self._detect_repeated_phrases["params"]["threshold_type"] = threshold_type
        self._detect_repeated_phrases["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_phrases))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_sequences(
        self,
        length_of_sequence: int = 3,
        min_repetitions: int = 3,
        threshold: int = 3,
        threshold_type: str = "count",
        unit: str = "character",
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_sequences = self._task_configs[
            "detect_repeated_sequences"
        ]
        self._detect_repeated_sequences["params"][
            "length_of_sequence"
        ] = length_of_sequence
        self._detect_repeated_sequences["params"]["min_repetitions"] = min_repetitions
        self._detect_repeated_sequences["params"]["threshold"] = threshold
        self._detect_repeated_sequences["params"]["threshold_type"] = threshold_type
        self._detect_repeated_sequences["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._detect_repeated_sequences))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_words(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 3,
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_words = self._task_configs["detect_repeated_words"]
        self._detect_repeated_words["params"]["threshold"] = threshold
        self._detect_repeated_words["params"]["threshold_type"] = threshold_type
        self._detect_repeated_words["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_words))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_short_reviews(
        self, threshold: int = 3
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_short_reviews = self._task_configs["detect_short_reviews"]
        self._detect_short_reviews["params"]["threshold"] = threshold
        self._tasks.append(self._task_builder.build(self._detect_short_reviews))
        return self

    def build(self) -> DataQualityAssessmentStageBuilder:
        """
        Builds the Ingest stage by validating configurations, constructing datasets,
        and assembling tasks.

        Returns:
            DataQualityAssessmentStageBuilder: The builder instance with the constructed stage.
        """
        self._validate()
        self._stage = DataQualityAssessmentStage(
            source_config=self._source_config,
            tasks=self._tasks,
            state=self._state,
            repo=self._repo,
            dataset_builder=self._dataset_builder,
            spark=self._spark,
        )
        return self

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
                "detect_control_chars is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_review_ids is None:
            errors.append(
                "detect_duplicate_review_ids is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_reviews is None:
            errors.append(
                "detect_duplicate_reviews is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_rows is None:
            errors.append(
                "detect_duplicate_rows is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_elongation is None:
            errors.append(
                "detect_elongation is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_emails is None:
            errors.append(
                "detect_emails is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_excess_whitespace is None:
            errors.append(
                "detect_excess_whitespace is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_html is None:
            errors.append(
                "detect_html is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_categories is None:
            errors.append(
                "detect_invalid_categories is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_ratings is None:
            errors.append(
                "detect_invalid_ratings is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_review_dates is None:
            errors.append(
                "detect_invalid_review_dates is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_phone_numbers is None:
            errors.append(
                "detect_phone_numbers is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_urls is None:
            errors.append(
                "detect_urls is a required step in the DataQualityAssessment Stage."
            )

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
