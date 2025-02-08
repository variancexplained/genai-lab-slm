#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/dqa/builder.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 1st 2025 05:01:45 am                                              #
# Modified   : Saturday February 8th 2025 09:00:11 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Acquire Stage Builder Module"""
from __future__ import annotations

from typing import Optional

from genailab.asset.dataset.config import DatasetConfig
from genailab.core.dtypes import DFType
from genailab.core.flow import PhaseDef, StageDef
from genailab.flow.base.builder import StageBuilder
from genailab.flow.dataprep.dqa.stage import DataQualityAssessmentStage
from genailab.flow.dataprep.operators.partition import PartitionTask
from genailab.flow.dataprep.operators.progress import ProgressTask
from genailab.flow.dataprep.operators.sample import SampleDataFrameTask
from genailab.infra.utils.data.partition import SparkPartitioner


# ------------------------------------------------------------------------------------------------ #
class DataQualityAssessmentStageBuilder(StageBuilder):

    __PHASE = PhaseDef.DATAPREP
    __STAGE = StageDef.DQA
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
        self._source_config = self._get_dataset_config(
            phase=self.phase, stage=self.stage, config="source_config"
        )
        self._target_config = self._get_dataset_config(
            phase=self.phase, stage=self.stage, config="target_config"
        )

        self._detect_accents = None
        self._detect_control_chars = None
        self._detect_duplicate_review_ids = None
        self._detect_duplicate_reviews = None
        self._detect_duplicate_rows = None
        self._detect_elongation = None
        self._detect_emails = None
        self._detect_special_chars = None
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

        self._task_configs = self._get_stage_config(
            phase=self.__PHASE, stage=self.__STAGE, config="tasks"
        )


    def partition_dataset(self)-> DataQualityAssessmentStageBuilder:
        config = self._get_app_config(section='spark')
        partitioner = SparkPartitioner(target_partition_size=config.target_partition_size)
        task = PartitionTask(partitioner=partitioner)
        self._tasks.append(task)
        return self
    # -------------------------------------------------------------------------------------------- #
    def sample_dataset(self, z: float = 1.96, p: float = 0.5, moe: float = 0.01)-> DataQualityAssessmentStageBuilder:
        task = SampleDataFrameTask(z=z, p=p, moe=moe)
        self._tasks.append(task)
        return self
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
    def detect_non_english(self, fast: bool = True) -> DataQualityAssessmentStageBuilder:
        self.detect_non_english_app_names(fast=fast)
        self.detect_non_english_reviews(fast=fast)
        return self

    def detect_non_english_app_names(self, fast: bool = True) -> None:
        self._detect_non_english_app_names = self._task_configs[
            "detect_non_english_app_names"
        ]
        self._detect_non_english_app_names["params"]["fast"] = fast
        self._tasks.append(self._task_builder.build(self._detect_non_english_app_names))

    def detect_non_english_reviews(self, fast: bool = True) -> None:
        self._detect_non_english_reviews = self._task_configs[
            "detect_non_english_reviews"
        ]
        self._detect_non_english_reviews["params"]["fast"] = fast
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
    def detect_special_chars(self) -> DataQualityAssessmentStageBuilder:
        self._detect_special_chars = self._task_configs["detect_special_chars"]
        self._tasks.append(self._task_builder.build(self._detect_special_chars))
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
    def detect_repeated_phrases(
        self,
        length_of_phrase: int = 2,
        threshold: int = 3,
        max_repetitions: int = 1,
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_phrases = self._task_configs["detect_repeated_phrases"]
        self._detect_repeated_phrases["params"]["length_of_phrase"] = length_of_phrase
        self._detect_repeated_phrases["params"]["threshold"] = threshold
        self._detect_repeated_phrases["params"]["max_repetitions"] = max_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_phrases))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_sequences(
        self,
        length_of_sequence: int = 3,
        threshold: int = 3,
        max_repetitions: int = 1,
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_sequences = self._task_configs[
            "detect_repeated_sequences"
        ]
        self._detect_repeated_sequences["params"][
            "length_of_sequence"
        ] = length_of_sequence
        self._detect_repeated_sequences["params"]["threshold"] = threshold
        self._detect_repeated_sequences["params"]["max_repetitions"] = max_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_sequences))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_repeated_words(
        self,
        threshold: int = 3,
        max_repetitions: int = 1,
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_repeated_words = self._task_configs["detect_repeated_words"]
        self._detect_repeated_words["params"]["threshold"] = threshold
        self._detect_repeated_words["params"]["max_repetitions"] = max_repetitions
        self._tasks.append(self._task_builder.build(self._detect_repeated_words))

        return self

    # -------------------------------------------------------------------------------------------- #
    def detect_short_reviews(
        self, threshold: int = 10
    ) -> DataQualityAssessmentStageBuilder:
        self._detect_short_reviews = self._task_configs["detect_short_reviews"]
        self._detect_short_reviews["params"]["threshold"] = threshold
        self._tasks.append(self._task_builder.build(self._detect_short_reviews))
        return self

    # -------------------------------------------------------------------------------------------- #
    def progress(self) -> DataQualityAssessmentStageBuilder:
        # Add the progress task that triggers the computation and shows progress
        # partition-wise. This should not be performed on large datasets.
        task = ProgressTask(spark=self._spark)
        self._tasks.append(task)
        return self

    # -------------------------------------------------------------------------------------------- #
    def build(
        self,
        source_config: Optional[DatasetConfig] = None,
        target_config: Optional[DatasetConfig] = None,
        strict: bool = True,
    ) -> DataQualityAssessmentStage:
        """
        Builds the preprocess stage by validating configurations and assembling tasks.

        Args:
            source_config (Optional[DatasetConfig]): An optional configuration object for
                the source dataset. If not provided, the method falls back to the source
                configuration defined in the stage YAML config.
            target_config (Optional[DatasetConfig]): An optional configuration object for
                the target dataset. If not provided, the method falls back to the target
                configuration defined in the stage YAML config.
            strict (bool): Whether strict, more thorough validation during build process.
        Returns:
            Stage: The DataQualityAssessmentStage object.
        """
        self._validate(strict=strict)

        self._spark = self._get_spark(dftype=self.dftype)

        stage = DataQualityAssessmentStage(
            source_config=source_config or self._source_config,
            target_config=target_config or self._target_config,
            tasks=self._tasks,
            repo=self._repo,
            dataset_builder=self._dataset_builder,
            spark=self._spark,
        )
        self.reset()
        return stage

    def _validate(self, strict: bool = False) -> None:
        """
        Validates the configurations and settings for the Preprocess stage.

        Ensures that required fields such as the source filepath, encoding, datatypes,
        and datetime conversion tasks are defined.

        Args:
            strict (bool): Whether strict, more thorough validation during build process.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        super()._validate()
        errors = []
        if self._detect_control_chars is None and strict:
            errors.append(
                "detect_control_chars is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_review_ids is None and strict:
            errors.append(
                "detect_duplicate_review_ids is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_reviews is None and strict:
            errors.append(
                "detect_duplicate_reviews is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_duplicate_rows is None and strict:
            errors.append(
                "detect_duplicate_rows is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_elongation is None and strict:
            errors.append(
                "detect_elongation is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_emails is None and strict:
            errors.append(
                "detect_emails is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_excess_whitespace is None and strict:
            errors.append(
                "detect_excess_whitespace is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_html is None and strict:
            errors.append(
                "detect_html is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_categories is None and strict:
            errors.append(
                "detect_invalid_categories is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_ratings is None and strict:
            errors.append(
                "detect_invalid_ratings is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_invalid_review_dates is None and strict:
            errors.append(
                "detect_invalid_review_dates is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_phone_numbers is None and strict:
            errors.append(
                "detect_phone_numbers is a required step in the DataQualityAssessment Stage."
            )
        if self._detect_urls is None and strict:
            errors.append(
                "detect_urls is a required step in the DataQualityAssessment Stage."
            )

        if errors:
            self.reset()
            msg = "\n".join(errors)
            self._logger.error(msg)
            raise ValueError(msg)
