#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_features/test_tqa_syntactic.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 11:07:32 pm                                             #
# Modified   : Wednesday January 29th 2025 10:08:17 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest
from genailab.asset.dataset.config import DatasetConfig
from genailab.flow.feature.tqa.syntactic.builder import TQASyntacticStageBuilder
from genailab.infra.config.flow import FlowConfigReader

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
@pytest.mark.tqa1
class TestTQASyntactic:  # pragma: no cover
    """Tests with source and target configurations passed to the builder."""

    # ============================================================================================ #
    def test_setup(self, container, spark, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Remove target dataset if it exists
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "feature"
        ]["stages"]["tqa_syntactic"]["target_config"]
        target_config = DatasetConfig.from_dict(config=config)
        # Remove the dataset if it exists
        repo = container.io.repo()
        asset_id = repo.get_asset_id(
            phase=target_config.phase, stage=target_config.stage, name=target_config.name
        )
        repo.remove(asset_id=asset_id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_tqa_syntactic_full(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain the tqa_syntactic configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "feature"
        ]["stages"]["tqa_syntactic"]
        # Configure the Source and Target Configs
        source_config = DatasetConfig.from_dict(config["source_config"])
        target_config = DatasetConfig.from_dict(config["target_config"])

        stage = (
            TQASyntacticStageBuilder()
            .noun_phrase_count()
            .adjective_noun_pair_count()
            .aspect_verb_pair_count()
            .adverb_phrase_count()
            .noun_count()
            .verb_count()
            .adverb_count()
            .adjective_count()
            .build(source_config=source_config, target_config=target_config)
        )
        target = stage.run()

        logging.info(f"\n\nTQA Syntactic Passport\n{target.passport}")
        logging.info(f"\n\nTQA Syntactic Files\n{target.files}")
        logging.info(f"\n\nTQA Syntactic Status\n{target.status}")
        logging.info(f"\n\nTQA Syntactic Results\n{target.dataframe}")



        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
