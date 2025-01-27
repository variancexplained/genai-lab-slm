#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/clean/builder.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Monday January 27th 2025 04:45:28 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Data Cleaning Stage Builder Module"""
from __future__ import annotations
from typing import Optional

from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.dataprep.clean.stage import DataCleaningStage


# ------------------------------------------------------------------------------------------------ #
class DataCleaningStageBuilder(StageBuilder):

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.SEMICLEAN
    __DFTYPE = DFType.SPARK

    def __init__(self) -> None:
        super().__init__()
        self.reset()
    @property
    def phase(self) -> PhaseDef:
        """
        The phase of the pipeline associated with the preprocess stage.

        Returns:
            PhaseDef: The phase associated with the pipeline.
        """
        return self.__PHASE

    @property
    def stage(self) -> StageDef:
        """
        The stage of the pipeline associated with the preprocess stage.

        Returns:
            StageDef: The stage associated with the pipeline.
        """
        return self.__STAGE

    @property
    def dftype(self) -> DFType:
        """
        Defines the dataframe type of the pipeline.

        Returns:
            DFType: The dataframe type used in the pipeline.
        """
        return self.__DFTYPE
    def reset(self) -> None:
        super().reset()
        self._source_config = None
        self._target_config = None
        self._clean_accents = None
        self._clean_control_chars = None
        self._clean_duplicate_review_ids = None
        self._clean_duplicate_reviews = None
        self._clean_duplicate_rows = None
        self._clean_elongation = None
        self._clean_emails = None
        self._clean_special_chars = None
        self._clean_excess_special_chars = None
        self._clean_excess_whitespace = None
        self._clean_html = None
        self._clean_invalid_categories = None
        self._clean_invalid_ratings = None
        self._clean_invalid_review_dates = None
        self._clean_less_than_threshold = None
        self._clean_non_english_app_names = None
        self._clean_non_english_reviews = None
        self._clean_phone_numbers = None
        self._clean_repeated_chars = None
        self._clean_repeated_phrases = None
        self._clean_repeated_sequences = None
        self._clean_repeated_words = None
        self._clean_short_reviews = None
        self._clean_urls = None

        self._task_configs = self._get_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )

    # -------------------------------------------------------------------------------------------- #
    #                                 SOURCE AND TARGET DATASETS                                   #
    # -------------------------------------------------------------------------------------------- #
    def source(self, source_config: DatasetConfig) -> DataCleaningStageBuilder:
        self._source_config = source_config
        return self

    # -------------------------------------------------------------------------------------------- #
    def target(self, target_config: DatasetConfig) -> DataCleaningStageBuilder:
        self._target_config = target_config
        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_privacy_issues(self) -> DataCleaningStageBuilder:
        self.clean_emails()
        self.clean_phone_numbers()
        self.clean_urls()
        return self

    def clean_urls(self) -> None:
        self._clean_urls = self._task_configs["clean_urls"]
        self._tasks.append(self._task_builder.build(self._clean_urls))

    def clean_phone_numbers(self) -> None:
        self._clean_phone_numbers = self._task_configs["clean_phone_numbers"]
        self._tasks.append(self._task_builder.build(self._clean_phone_numbers))

    def clean_emails(self) -> None:
        self._clean_emails = self._task_configs["clean_emails"]
        self._tasks.append(self._task_builder.build(self._clean_emails))

    # -------------------------------------------------------------------------------------------- #
    def clean_duplication(self) -> DataCleaningStageBuilder:
        self.clean_duplicate_review_ids()
        self.clean_duplicate_reviews()
        self.clean_duplicate_rows()
        return self

    def clean_duplicate_review_ids(self) -> None:
        self._clean_duplicate_review_ids = self._task_configs[
            "clean_duplicate_review_ids"
        ]
        self._tasks.append(self._task_builder.build(self._clean_duplicate_review_ids))

    def clean_duplicate_reviews(self) -> None:
        self._clean_duplicate_reviews = self._task_configs["clean_duplicate_reviews"]
        self._tasks.append(self._task_builder.build(self._clean_duplicate_reviews))

    def clean_duplicate_rows(self) -> None:
        self._clean_duplicate_rows = self._task_configs["clean_duplicate_rows"]
        self._tasks.append(self._task_builder.build(self._clean_duplicate_rows))

    # -------------------------------------------------------------------------------------------- #
    def clean_invalid_characters(self) -> DataCleaningStageBuilder:
        self.clean_accents()
        self.clean_control_chars()
        self.clean_html()
        return self

    def clean_accents(self) -> None:
        self._clean_accents = self._task_configs["clean_accents"]
        self._tasks.append(self._task_builder.build(self._clean_accents))

    def clean_control_chars(self) -> None:
        self._clean_control_chars = self._task_configs["clean_control_chars"]
        self._tasks.append(self._task_builder.build(self._clean_control_chars))

    def clean_html(self) -> None:
        self._clean_html = self._task_configs["clean_html"]
        self._tasks.append(self._task_builder.build(self._clean_html))

    # -------------------------------------------------------------------------------------------- #
    def clean_invalid_values(self) -> DataCleaningStageBuilder:
        self.clean_invalid_categories()
        self.clean_invalid_ratings()
        self.clean_invalid_review_dates()
        return self

    def clean_invalid_categories(self) -> None:
        self._clean_invalid_categories = self._task_configs["clean_invalid_categories"]
        self._tasks.append(self._task_builder.build(self._clean_invalid_categories))

    def clean_invalid_ratings(self) -> None:
        self._clean_invalid_ratings = self._task_configs["clean_invalid_ratings"]
        self._tasks.append(self._task_builder.build(self._clean_invalid_ratings))

    def clean_invalid_review_dates(
        self, range_min: int = 2008, range_max: int = 2024, range_type: str = "year"
    ) -> None:
        self._clean_invalid_review_dates = self._task_configs[
            "clean_invalid_review_dates"
        ]
        self._clean_invalid_review_dates["params"]["ramge_min"] = range_min
        self._clean_invalid_review_dates["params"]["ramge_max"] = range_max
        self._clean_invalid_review_dates["params"]["ramge_type"] = range_type
        self._tasks.append(self._task_builder.build(self._clean_invalid_review_dates))

    # -------------------------------------------------------------------------------------------- #
    def clean_non_english(self) -> DataCleaningStageBuilder:
        self.clean_non_english_app_names()
        self.clean_non_english_reviews()
        return self

    def clean_non_english_app_names(self) -> None:
        self._clean_non_english_app_names = self._task_configs[
            "clean_non_english_app_names"
        ]
        self._tasks.append(self._task_builder.build(self._clean_non_english_app_names))

    def clean_non_english_reviews(self) -> None:
        self._clean_non_english_reviews = self._task_configs[
            "clean_non_english_reviews"
        ]
        self._tasks.append(self._task_builder.build(self._clean_non_english_reviews))

    # -------------------------------------------------------------------------------------------- #
    def clean_elongation(
        self, threshold: int = 3, max_elongation: int = 2
    ) -> DataCleaningStageBuilder:
        self._clean_elongation = self._task_configs["clean_elongation"]
        self._clean_elongation["params"]["threshold"] = threshold
        self._clean_elongation["params"]["max_elongation"] = max_elongation
        self._tasks.append(self._task_builder.build(self._clean_elongation))
        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_special_chars(self) -> DataCleaningStageBuilder:
        self._clean_special_chars = self._task_configs["clean_special_chars"]
        self._tasks.append(self._task_builder.build(self._clean_special_chars))
        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_excess_special_chars(
        self,
        threshold: float = 0.35,
        threshold_type: str = "proportion",
        unit: str = "character",
    ) -> DataCleaningStageBuilder:
        self._clean_excess_special_chars = self._task_configs[
            "clean_excess_special_chars"
        ]
        self._clean_excess_special_chars["params"]["threshold"] = threshold
        self._clean_excess_special_chars["params"]["threshold_type"] = threshold_type
        self._clean_excess_special_chars["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._clean_excess_special_chars))
        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_excess_whitespace(self) -> DataCleaningStageBuilder:
        self._clean_excess_whitespace = self._task_configs["clean_excess_whitespace"]
        self._tasks.append(self._task_builder.build(self._clean_excess_whitespace))
        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_repeated_chars(
        self, min_repetitions: int = 4
    ) -> DataCleaningStageBuilder:
        self._clean_repeated_chars = self._task_configs["clean_repeated_chars"]
        self._clean_repeated_chars["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._clean_repeated_chars))

        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_repeated_phrases(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 2,
    ) -> DataCleaningStageBuilder:
        self._clean_repeated_phrases = self._task_configs["clean_repeated_phrases"]
        self._clean_repeated_phrases["params"]["threshold"] = threshold
        self._clean_repeated_phrases["params"]["threshold_type"] = threshold_type
        self._clean_repeated_phrases["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._clean_repeated_phrases))

        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_repeated_sequences(
        self,
        length_of_sequence: int = 3,
        min_repetitions: int = 3,
        threshold: int = 3,
        threshold_type: str = "count",
        unit: str = "character",
    ) -> DataCleaningStageBuilder:
        self._clean_repeated_sequences = self._task_configs["clean_repeated_sequences"]
        self._clean_repeated_sequences["params"][
            "length_of_sequence"
        ] = length_of_sequence
        self._clean_repeated_sequences["params"]["min_repetitions"] = min_repetitions
        self._clean_repeated_sequences["params"]["threshold"] = threshold
        self._clean_repeated_sequences["params"]["threshold_type"] = threshold_type
        self._clean_repeated_sequences["params"]["unit"] = unit
        self._tasks.append(self._task_builder.build(self._clean_repeated_sequences))

        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_repeated_words(
        self,
        threshold: int = 1,
        threshold_type: str = "count",
        min_repetitions: int = 3,
    ) -> DataCleaningStageBuilder:
        self._clean_repeated_words = self._task_configs["clean_repeated_words"]
        self._clean_repeated_words["params"]["threshold"] = threshold
        self._clean_repeated_words["params"]["threshold_type"] = threshold_type
        self._clean_repeated_words["params"]["min_repetitions"] = min_repetitions
        self._tasks.append(self._task_builder.build(self._clean_repeated_words))

        return self

    # -------------------------------------------------------------------------------------------- #
    def clean_short_reviews(self, threshold: int = 3) -> DataCleaningStageBuilder:
        self._clean_short_reviews = self._task_configs["clean_short_reviews"]
        self._clean_short_reviews["params"]["threshold"] = threshold
        self._tasks.append(self._task_builder.build(self._clean_short_reviews))
        return self

    def build(
        self,
        source_config: Optional[DatasetConfig] = None,
        target_config: Optional[DatasetConfig] = None,
        strict: bool = True,
        ) -> DataCleaningStageBuilder:
        """
        Builds the Preprocess stage by validating configurations, constructing datasets,
        and assembling tasks.

        Args:
            source_config (Optional[DatasetConfig]): An optional configuration object for
                the source dataset. If not provided, the method falls back to the source
                configuration defined in the stage YAML config.
            target_config (Optional[DatasetConfig]): An optional configuration object for
                the target dataset. If not provided, the method falls back to the target
                configuration defined in the stage YAML config.
            strict (bool): Whether strict, more thorough validation during build process.

        Returns:
            DataCleaningStageBuilder: The builder instance with the constructed stage.
        """
        self._validate(strict=strict)

        # Obtain a spark session
        self._spark = self._get_spark(dftype=self.dftype)

        stage = DataCleaningStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=self._tasks,
            repo=self._repo,
            dataset_builder=self._dataset_builder,
            spark=self._spark,
        )
        self.reset()
        return stage

    def _validate(self, strict: bool = True) -> None:
        """
        Validates the configurations and settings for the DataCleaning stage.

        Ensures that required fields such as the source filepath, encoding, datatypes,
        and datetime conversion tasks are defined.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if self._clean_non_english_app_names is None and strict is True:
            errors.append(
                "clean_non_english_app_names is a required step in the DataCleaning Stage."
            )
        if self._clean_non_english_reviews is None and strict is True:
            errors.append(
                "clean_non_english_reviews is a required step in the DataCleaning Stage."
            )
        if self._clean_accents is None and strict is True:
            errors.append("clean_accents is a required step in the DataCleaning Stage.")
        if self._clean_control_chars is None and strict is True:
            errors.append(
                "clean_control_chars is a required step in the DataCleaning Stage."
            )
        if self._clean_duplicate_review_ids is None and strict is True:
            errors.append(
                "clean_duplicate_review_ids is a required step in the DataCleaning Stage."
            )
        if self._clean_elongation is None and strict is True:
            errors.append(
                "clean_elongation is a required step in the DataCleaning Stage."
            )
        if self._clean_emails is None and strict is True:
            errors.append("clean_emails is a required step in the DataCleaning Stage.")
        if self._clean_excess_whitespace is None and strict is True:
            errors.append(
                "clean_excess_whitespace is a required step in the DataCleaning Stage."
            )
        if self._clean_html is None and strict is True:
            errors.append("clean_html is a required step in the DataCleaning Stage.")
        if self._clean_phone_numbers is None and strict is True:
            errors.append(
                "clean_phone_numbers is a required step in the DataCleaning Stage."
            )
        if self._clean_urls is None and strict is True:
            errors.append("clean_urls is a required step in the DataCleaning Stage.")

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
