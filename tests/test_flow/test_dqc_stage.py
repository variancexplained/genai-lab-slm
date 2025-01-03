#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dqc_stage.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday January 3rd 2025 01:03:57 am                                                 #
# Modified   : Friday January 3rd 2025 02:10:42 am                                                 #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.dataset.builder import DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.dataprep.dqc.builder import DataQualityCheckStageBuilder

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dqc
class TestDQCStage:  # pragma: no cover
    # ============================================================================================ #
    def test_build_run(self, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQC)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )
        # Construct the stage
        builder = DataQualityCheckStageBuilder()
        stage = (
            builder.source_passport(source_passport)
            .target_passport(target_passport)
            .detect_emails()
            .detect_phone_numbers()
            .detect_urls()
            .detect_non_english_app_names()
            .detect_non_english_reviews()
            .detect_short_reviews(threshold=3)
            .detect_duplicate_review_ids()
            .detect_duplicate_reviews()
            .detect_duplicate_rows()
            .detect_accents()
            .detect_control_chars()
            .detect_elongation(threshold=4, max_elongation=3)
            .detect_excess_special_chars(
                threshold=0.35, threshold_type="proportion", unit="character"
            )
            .detect_excess_whitespace()
            .detect_html()
            .detect_invalid_categories()
            .detect_invalid_ratings()
            .detect_invalid_review_dates(
                range_min=2020, range_max=2024, range_type="year"
            )
            .detect_repeated_chars(min_repetitions=4)
            .detect_repeated_phrases(
                threshold=1, threshold_type="count", min_repetitions=2
            )
            .detect_repeated_sequences(
                length_of_sequence=2,
                min_repetitions=3,
                threshold=3,
                threshold_type="count",
                unit="character",
            )
            .detect_repeated_words(
                threshold=1, threshold_type="count", min_repetitions=3
            )
            .build()
            .stage
        )

        # Run the stage
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.dataframe, pd.DataFrame)
        logging.info(target.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_source_passport(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQC)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )
        # Test source passport
        builder = DataQualityCheckStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.target_passport(target_passport)
                .detect_emails()
                .detect_phone_numbers()
                .detect_urls()
                .detect_non_english_app_names()
                .detect_non_english_reviews()
                .detect_short_reviews(threshold=3)
                .detect_duplicate_review_ids()
                .detect_duplicate_reviews()
                .detect_duplicate_rows()
                .detect_accents()
                .detect_control_chars()
                .detect_elongation(threshold=4, max_elongation=3)
                .detect_excess_special_chars(
                    threshold=0.35, threshold_type="proportion", unit="character"
                )
                .detect_excess_whitespace()
                .detect_html()
                .detect_invalid_categories()
                .detect_invalid_ratings()
                .detect_invalid_review_dates(
                    range_min=2020, range_max=2024, range_type="year"
                )
                .detect_repeated_chars(min_repetitions=4)
                .detect_repeated_phrases(
                    threshold=1, threshold_type="count", min_repetitions=2
                )
                .detect_repeated_sequences(
                    length_of_sequence=2,
                    min_repetitions=3,
                    threshold=3,
                    threshold_type="count",
                    unit="character",
                )
                .detect_repeated_words(
                    threshold=1, threshold_type="count", min_repetitions=3
                )
                .build()
                .stage
            )

        # Test invalid source passport
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(2)
                .target_passport(target_passport)
                .detect_emails()
                .detect_phone_numbers()
                .detect_urls()
                .detect_non_english_app_names()
                .detect_non_english_reviews()
                .detect_short_reviews(threshold=3)
                .detect_duplicate_review_ids()
                .detect_duplicate_reviews()
                .detect_duplicate_rows()
                .detect_accents()
                .detect_control_chars()
                .detect_elongation(threshold=4, max_elongation=3)
                .detect_excess_special_chars(
                    threshold=0.35, threshold_type="proportion", unit="character"
                )
                .detect_excess_whitespace()
                .detect_html()
                .detect_invalid_categories()
                .detect_invalid_ratings()
                .detect_invalid_review_dates(
                    range_min=2020, range_max=2024, range_type="year"
                )
                .detect_repeated_chars(min_repetitions=4)
                .detect_repeated_phrases(
                    threshold=1, threshold_type="count", min_repetitions=2
                )
                .detect_repeated_sequences(
                    length_of_sequence=2,
                    min_repetitions=3,
                    threshold=3,
                    threshold_type="count",
                    unit="character",
                )
                .detect_repeated_words(
                    threshold=1, threshold_type="count", min_repetitions=3
                )
                .build()
                .stage
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_target_passport(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )

        # Test target passport
        builder = DataQualityCheckStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(source_passport)
                .detect_emails()
                .detect_phone_numbers()
                .detect_urls()
                .detect_non_english_app_names()
                .detect_non_english_reviews()
                .detect_short_reviews(threshold=3)
                .detect_duplicate_review_ids()
                .detect_duplicate_reviews()
                .detect_duplicate_rows()
                .detect_accents()
                .detect_control_chars()
                .detect_elongation(threshold=4, max_elongation=3)
                .detect_excess_special_chars(
                    threshold=0.35, threshold_type="proportion", unit="character"
                )
                .detect_excess_whitespace()
                .detect_html()
                .detect_invalid_categories()
                .detect_invalid_ratings()
                .detect_invalid_review_dates(
                    range_min=2020, range_max=2024, range_type="year"
                )
                .detect_repeated_chars(min_repetitions=4)
                .detect_repeated_phrases(
                    threshold=1, threshold_type="count", min_repetitions=2
                )
                .detect_repeated_sequences(
                    length_of_sequence=2,
                    min_repetitions=3,
                    threshold=3,
                    threshold_type="count",
                    unit="character",
                )
                .detect_repeated_words(
                    threshold=1, threshold_type="count", min_repetitions=3
                )
                .build()
                .stage
            )

        # Test invalid target passport
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(source_passport)
                .target_passport(2)
                .detect_emails()
                .detect_phone_numbers()
                .detect_urls()
                .detect_non_english_app_names()
                .detect_non_english_reviews()
                .detect_short_reviews(threshold=3)
                .detect_duplicate_review_ids()
                .detect_duplicate_reviews()
                .detect_duplicate_rows()
                .detect_accents()
                .detect_control_chars()
                .detect_elongation(threshold=4, max_elongation=3)
                .detect_excess_special_chars(
                    threshold=0.35, threshold_type="proportion", unit="character"
                )
                .detect_excess_whitespace()
                .detect_html()
                .detect_invalid_categories()
                .detect_invalid_ratings()
                .detect_invalid_review_dates(
                    range_min=2020, range_max=2024, range_type="year"
                )
                .detect_repeated_chars(min_repetitions=4)
                .detect_repeated_phrases(
                    threshold=1, threshold_type="count", min_repetitions=2
                )
                .detect_repeated_sequences(
                    length_of_sequence=2,
                    min_repetitions=3,
                    threshold=3,
                    threshold_type="count",
                    unit="character",
                )
                .detect_repeated_words(
                    threshold=1, threshold_type="count", min_repetitions=3
                )
                .build()
                .stage
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_detect_url_task(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQC)
            .parquet()
            .as_spark()
            .name("reviews")
            .build()
            .passport
        )
        # Test Detect URL
        builder = DataQualityCheckStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(source_passport)
                .target_passport(target_passport)
                .detect_emails()
                .detect_phone_numbers()
                .detect_non_english_app_names()
                .detect_non_english_reviews()
                .detect_short_reviews(threshold=3)
                .detect_duplicate_review_ids()
                .detect_duplicate_reviews()
                .detect_duplicate_rows()
                .detect_accents()
                .detect_control_chars()
                .detect_elongation(threshold=4, max_elongation=3)
                .detect_excess_special_chars(
                    threshold=0.35, threshold_type="proportion", unit="character"
                )
                .detect_excess_whitespace()
                .detect_html()
                .detect_invalid_categories()
                .detect_invalid_ratings()
                .detect_invalid_review_dates(
                    range_min=2020, range_max=2024, range_type="year"
                )
                .detect_repeated_chars(min_repetitions=4)
                .detect_repeated_phrases(
                    threshold=1, threshold_type="count", min_repetitions=2
                )
                .detect_repeated_sequences(
                    length_of_sequence=2,
                    min_repetitions=3,
                    threshold=3,
                    threshold_type="count",
                    unit="character",
                )
                .detect_repeated_words(
                    threshold=1, threshold_type="count", min_repetitions=3
                )
                .build()
                .stage
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
