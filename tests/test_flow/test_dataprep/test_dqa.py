#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dataprep/test_dqa.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 11:07:32 pm                                             #
# Modified   : Friday January 24th 2025 01:07:46 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.dataset.config import DatasetConfig
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetPassport
from discover.flow.dataprep.dqa.builder import DataQualityAssessmentStageBuilder
from discover.infra.config.flow import FlowConfigReader
from discover.infra.utils.file.fileset import FileSet

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dqa
class TestDQA:  # pragma: no cover
    """Tests with source and target configurations passed to the builder."""

    # ============================================================================================ #
    def test_setup(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Remove target dataset if it exists

        # Get the target configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["dqa"]["target_config"]
        config = DatasetConfig.from_dict(config=config)
        # Remove the dataset if it exists
        repo = container.io.repo()
        asset_id = repo.get_asset_id(
            phase=config.phase, stage=config.stage, name=config.name
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
    def test_dqa(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain the dqa configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["dqa"]
        # Configure the Source and Target Configs
        source_config = DatasetConfig.from_dict(config["source_config"])
        # Change the name of the target
        target_config_dict = config["target_config"]
        target_config_dict["name"] = self.__class__.__name__
        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_non_english()
            .detect_privacy_issues()
            .detect_duplication()
            .detect_invalid_values()
            .detect_elongation(threshold=3, max_elongation=2)
            .detect_special_chars()
            .detect_invalid_characters()
            .detect_excess_special_chars()
            .detect_repeated_words()
            .detect_repeated_sequences()
            .detect_repeated_phrases()
            .detect_short_reviews()
            .detect_excess_whitespace()
            .build(source_config=source_config, target_config=target_config)
        )
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert isinstance(target.info, (pd.DataFrame, pd.core.frame.DataFrame))
        assert isinstance(target.summary, dict)
        assert target.name == self.__class__.__name__

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())
        logging.info(f"\n\n{target.info}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dqa
class TestDQAFromYAML:  # pragma: no cover
    """Tests with source and target configurations read from YAML."""

    # ============================================================================================ #
    def test_setup(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Remove target dataset if it exists

        # Get the target configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["dqa"]["target_config"]
        config = DatasetConfig.from_dict(config=config)
        # Remove the dataset if it exists
        repo = container.io.repo()
        asset_id = repo.get_asset_id(
            phase=config.phase, stage=config.stage, name=config.name
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
    def test_dqa(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_non_english()
            .detect_privacy_issues()
            .detect_duplication()
            .detect_invalid_values()
            .detect_elongation(threshold=3, max_elongation=2)
            .detect_special_chars()
            .detect_invalid_characters()
            .detect_excess_special_chars()
            .detect_repeated_words()
            .detect_repeated_sequences()
            .detect_repeated_phrases()
            .detect_short_reviews()
            .detect_excess_whitespace()
            .build()
        )
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert isinstance(target.info, (pd.DataFrame, pd.core.frame.DataFrame))
        assert isinstance(target.summary, dict)

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())
        logging.info(f"\n\n{target.info}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
