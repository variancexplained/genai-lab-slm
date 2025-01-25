#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dataprep/test_dqa.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 11:07:32 pm                                             #
# Modified   : Saturday January 25th 2025 04:41:13 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest
from genailabslm.asset.dataset.config import DatasetConfig
from genailabslm.asset.dataset.dataset import Dataset
from genailabslm.asset.dataset.identity import DatasetPassport
from genailabslm.core.dtypes import DFType
from genailabslm.flow.dataprep.dqa.builder import DataQualityAssessmentStageBuilder
from genailabslm.infra.config.flow import FlowConfigReader
from genailabslm.infra.utils.file.fileset import FileSet
from pyspark.sql import DataFrame

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
    def test_setup(self, container, spark, caplog) -> None:
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
    @pytest.mark.skip(
        reason="Full pipeline is working. Unless changes, skip and run in non-strict mode in the next method"
    )
    def test_dqa_full(self, container, spark, caplog) -> None:
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

        repo = container.io.repo()
        source_asset_id = repo.get_asset_id(
            phase=source_config.phase,
            stage=source_config.stage,
            name=source_config.name,
        )
        source = repo.get(asset_id=source_asset_id, dftype=DFType.PANDAS)

        # Source Dataset
        assert isinstance(source, Dataset)
        assert isinstance(source.passport, DatasetPassport)
        assert isinstance(source.file, FileSet)
        assert isinstance(source.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert source.name == source_config.name
        assert source.consumed
        assert source.published

        logging.info(f"\n\nSource Dataset Information\n{'='*40}")
        logging.info(f"\nSource Passport\n{source.passport}")
        logging.info(f"\nSource State\n{source.state}")
        logging.info(f"\nSource File\n{source.file}")
        logging.info(f"\nSource Event Log{source.eventlog}\n")
        logging.info(f"\nSource Dataframe{source.dataframe.head(5)}\n")

        # Target Dataset
        logging.info("Target Assertion Test: Dataset Type")
        assert isinstance(target, Dataset)
        logging.info("Target Assertion Test: Passport Type")
        assert isinstance(target.passport, DatasetPassport)
        logging.info("Target Assertion Test: Fileset Type")
        assert isinstance(target.file, FileSet)
        logging.info("Target Assertion Test: DataFrame Type")
        assert isinstance(target.dataframe, DataFrame)
        assert target.name == target_config.name
        assert not target.consumed
        assert target.published

        logging.info(f"\n\nTarget Dataset Information\n{'='*40}")
        logging.info(f"\nTarget Passport\n{target.passport}")
        logging.info(f"\nTarget State\n{target.state}")
        logging.info(f"\nTarget File\n{target.file}")
        logging.info(f"\nTarget Event Log{target.eventlog}\n")
        logging.info(f"\nTarget Dataframe{target.dataframe.head(5)}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_non_strict(self, container, spark, caplog) -> None:
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

        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_excess_whitespace()
            .build(
                source_config=source_config, target_config=target_config, strict=False
            )
        )
        target = stage.run()

        repo = container.io.repo()
        source_asset_id = repo.get_asset_id(
            phase=source_config.phase,
            stage=source_config.stage,
            name=source_config.name,
        )
        source = repo.get(asset_id=source_asset_id, dftype=DFType.PANDAS)

        # Source Dataset
        assert isinstance(source, Dataset)
        assert isinstance(source.passport, DatasetPassport)
        assert isinstance(source.file, FileSet)
        assert isinstance(source.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert source.name == source_config.name
        assert source.consumed
        assert source.published

        logging.info(f"\n\nSource Dataset Information\n{'='*40}")
        logging.info(f"\nSource Passport\n{source.passport}")
        logging.info(f"\nSource State\n{source.state}")
        logging.info(f"\nSource File\n{source.file}")
        logging.info(f"\nSource Event Log{source.eventlog}\n")
        logging.info(f"\nSource Dataframe{source.dataframe.head(5)}\n")

        # Target Dataset
        logging.info("Target Assertion Test: Dataset Type")
        assert isinstance(target, Dataset)
        logging.info("Target Assertion Test: Passport Type")
        assert isinstance(target.passport, DatasetPassport)
        logging.info("Target Assertion Test: Fileset Type")
        assert isinstance(target.file, FileSet)
        logging.info("Target Assertion Test: DataFrame Type")
        assert isinstance(target.dataframe, DataFrame)
        assert target.name == target_config.name
        assert not target.consumed
        assert target.published

        logging.info(f"\n\nTarget Dataset Information\n{'='*40}")
        logging.info(f"\nTarget Passport\n{target.passport}")
        logging.info(f"\nTarget State\n{target.state}")
        logging.info(f"\nTarget File\n{target.file}")
        logging.info(f"\nTarget Event Log{target.eventlog}\n")
        logging.info(f"\nTarget Dataframe{target.dataframe.head(5)}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_cache(self, caplog) -> None:
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

        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_privacy_issues()
            .detect_duplication()
            .detect_excess_whitespace()
            .build(
                source_config=source_config, target_config=target_config, strict=False
            )
        )
        logging.info(f"\n\nTesting Cache. No Task Log\n{'='*40}")
        target = stage.run()

        # Target Dataset
        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, DataFrame)
        assert target.name == target_config.name
        assert not target.consumed
        assert target.published

        logging.info(f"\n\nTarget Dataset Information\n{'='*40}")
        logging.info(f"\nTarget Passport\n{target.passport}")
        logging.info(f"\nTarget State\n{target.state}")
        logging.info(f"\nTarget File\n{target.file}")
        logging.info(f"\nTarget Event Log{target.eventlog}\n")
        logging.info(f"\nTarget Dataframe{target.dataframe.head(5)}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_force(self, caplog) -> None:
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

        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_privacy_issues()
            .detect_duplication()
            .detect_excess_whitespace()
            .build(
                source_config=source_config, target_config=target_config, strict=False
            )
        )

        logging.info(f"\n\nTesting Force. Forcing Execution\n{'='*40}")
        target = stage.run(force=True)

        # Target Dataset
        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, DataFrame)
        assert target.name == target_config.name
        assert not target.consumed
        assert target.published

        logging.info(f"\n\nTarget Dataset Information\n{'='*40}")
        logging.info(f"\nTarget Passport\n{target.passport}")
        logging.info(f"\nTarget File\n{target.file}")
        logging.info(f"\nTarget Event Log{target.eventlog}\n")
        logging.info(f"\nTarget Dataframe{target.dataframe.head(5)}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_strict_exception(self, container, spark, caplog) -> None:
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

        target_config = DatasetConfig.from_dict(target_config_dict)

        with pytest.raises(ValueError):

            _ = (
                DataQualityAssessmentStageBuilder()
                .detect_excess_whitespace()
                .build(source_config=source_config, target_config=target_config)
            )
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
    def test_setup(self, container, spark, caplog) -> None:
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
    def test_dqa_from_yaml(self, container, spark, caplog) -> None:
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

        # Change the name of the target
        target_config_dict = config["target_config"]
        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            DataQualityAssessmentStageBuilder()
            .detect_privacy_issues()
            .detect_duplication()
            .detect_excess_whitespace()
            .build(strict=False)
        )
        target = stage.run()

        # Target Dataset
        logging.info(f"\n\nTarget Dataset Evaluation\n{'='*40}")
        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, DataFrame)
        assert target.name == target_config.name
        assert not target.consumed
        assert target.published

        logging.info(f"\nTarget Passport\n{target.passport}")
        logging.info(f"\nTarget State\n{target.state}")
        logging.info(f"\nTarget File\n{target.file}")
        logging.info(f"\nTarget Event Log{target.eventlog}\n")
        logging.info(f"\nTarget Dataframe{target.dataframe.head(5)}\n")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
