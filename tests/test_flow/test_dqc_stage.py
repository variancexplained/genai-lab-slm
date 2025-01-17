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
# Created    : Thursday January 2nd 2025 06:30:50 pm                                               #
# Modified   : Thursday January 16th 2025 09:05:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest
from pyspark.sql import DataFrame

from discover.asset.dataset.dataset import Dataset
from discover.core.flow import PhaseDef, StageDef
from discover.flow.dataprep.dqa.builder import DataQualityCheckStageBuilder

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
    def test_build_run(self, workspace, flowstate, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the stage
        builder = DataQualityCheckStageBuilder()
        stage = (
            builder.spark(spark)
            .detect_privacy_issues()
            .detect_non_english()
            .detect_duplication()
            .detect_invalid_characters()
            .detect_invalid_values()
            .detect_short_reviews(threshold=3)
            .detect_elongation(threshold=4, max_elongation=3)
            .detect_excess_special_chars(
                threshold=0.35, threshold_type="proportion", unit="character"
            )
            .detect_excess_whitespace()
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
        assert isinstance(target.dataframe, DataFrame)
        logging.info(target.dataframe.head())
        logging.info(target.passport)
        logging.info(target.passport.source)

        # Check repository
        dataset = workspace.dataset_repo.get(
            asset_id=target.asset_id, dftype=stage.dftype, spark=spark
        )
        assert target == dataset

        # Remove it from the repository
        workspace.dataset_repo.remove(asset_id=target.asset_id)
        flowstate.delete(phase=PhaseDef.DATAPREP, stage=StageDef.DQC)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_tasks(self, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataQualityCheckStageBuilder()
        with pytest.raises(ValueError):
            _ = builder.spark(spark).build().stage

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
