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
# Modified   : Sunday January 19th 2025 01:01:00 pm                                                #
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
from discover.asset.dataset.identity import DatasetConfig
from discover.core.dtypes import DFType
from discover.core.file import FileFormat
from discover.core.flow import PhaseDef, StageDef
from discover.flow.dataprep.dqa.builder import DataQualityAssessmentStageBuilder

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
    def test_noun_phrase_count(
        self, workspace, flowstate, spark, clean_dataset_config, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the Target Dataset
        target = DatasetConfig(
            phase=PhaseDef.FEATURE,
            stage=StageDef.TQA_SYNTACTIC,
            name="review",
            file_format=FileFormat.PARQUET,
            asset_type="dataset",
            dftype=DFType.SPARKNLP,
        )
        # Construct the stage
        builder = DataQualityAssessmentStageBuilder()
        stage = (
            builder.spark(spark)
            .source(clean_dataset_config)
            .target(target)
            .noun_phrase_count()
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
