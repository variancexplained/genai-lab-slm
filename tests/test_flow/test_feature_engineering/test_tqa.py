#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_feature_engineering/test_tqa.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday January 19th 2025 12:26:24 pm                                                #
# Modified   : Sunday January 19th 2025 06:01:14 pm                                                #
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
from discover.flow.feature.tqa.syntactic.builder import TQASyntacticStageBuilder

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.tqa
class TestTQA:  # pragma: no cover
    # ============================================================================================ #
    def test_phrase_and_pos_counts(
        self, workspace, flowstate, sparknlp, clean_dataset_config, caplog
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
        builder = TQASyntacticStageBuilder()
        stage = (
            builder.spark(sparknlp)
            .source(clean_dataset_config)
            .target(target)
            .noun_phrase_count()
            .adjective_noun_pair_count()
            .aspect_verb_pair_count()
            .adverb_phrase_count()
            .noun_count()
            .verb_count()
            .adverb_count()
            .adjective_count()
            .build()
            .stage
        )

        # Run the stage
        target = stage.run(force=True)

        assert isinstance(target, Dataset)
        assert isinstance(target.dataframe, DataFrame)

        # Check counts
        logging.info(
            target.dataframe.select(
                "tqa_syntactic_noun_phrase_count",
                "tqa_syntactic_adjective_noun_pair_count",
                "tqa_syntactic_aspect_verb_pair_count",
                "tqa_syntactic_adverb_phrase_count",
                "tqa_syntactic_noun_count",
                "tqa_syntactic_verb_count",
                "tqa_syntactic_adverb_count",
                "tqa_syntactic_adjective_count",
            ).show(truncate=False, n=20)
        )

        # Check repository
        dataset = workspace.dataset_repo.get(
            asset_id=target.asset_id, dftype=stage.dftype, spark=sparknlp
        )
        assert target == dataset

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
