#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dataprep/test_ingest.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 11:07:32 pm                                             #
# Modified   : Sunday January 26th 2025 05:57:20 am                                                #
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
from genailabslm.asset.dataset.state import DatasetStateDef
from genailabslm.flow.dataprep.preprocess.builder import PreprocessStageBuilder
from genailabslm.infra.config.flow import FlowConfigReader
from genailabslm.infra.utils.file.fileset import FileSet

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.preprocess
class TestPreprocess:  # pragma: no cover
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
        ]["stages"]["preprocess"]["target_config"]
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
    def test_preprocess(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain the preprocess configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["preprocess"]
        # Configure the Source and Target Configs
        source_config = DatasetConfig.from_dict(config["source_config"])
        # Change the name of the target
        target_config_dict = config["target_config"]
        target_config_dict["name"] = self.__class__.__name__
        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            PreprocessStageBuilder()
            .encoding()
            .datatypes()
            .newlines()
            .datetime()
            .build(source_config=source_config, target_config=target_config)
        )
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert isinstance(target.status, DatasetStateDef)
        assert isinstance(target.get_registration(), dict)
        assert isinstance(target.eventlog, (pd.DataFrame, pd.core.frame.DataFrame))
        assert target.name == self.__class__.__name__

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())
        logging.info(f"\nState\n{target._state}")
        logging.info(f"\nRegistration\n{target.get_registration()}")
        logging.info(f"\nEvents\n{target.eventlog}")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_preprocess_cache(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain the preprocess configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["preprocess"]
        # Configure the Source and Target Configs
        source_config = DatasetConfig.from_dict(config["source_config"])
        # Change the name of the target
        target_config_dict = config["target_config"]
        target_config_dict["name"] = self.__class__.__name__
        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            PreprocessStageBuilder()
            .encoding()
            .datatypes()
            .newlines()
            .datetime()
            .build(source_config=source_config, target_config=target_config)
        )
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert target.name == self.__class__.__name__

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_preprocess_force(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Obtain the preprocess configuration
        config = FlowConfigReader().get_config(section="phases", namespace=False)[
            "dataprep"
        ]["stages"]["preprocess"]
        # Configure the Source and Target Configs
        source_config = DatasetConfig.from_dict(config["source_config"])
        # Change the name of the target
        target_config_dict = config["target_config"]
        target_config_dict["name"] = self.__class__.__name__
        target_config = DatasetConfig.from_dict(target_config_dict)

        stage = (
            PreprocessStageBuilder()
            .encoding()
            .datatypes()
            .newlines()
            .datetime()
            .build(source_config=source_config, target_config=target_config)
        )
        target = stage.run(force=True)

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))
        assert target.name == self.__class__.__name__

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.preprocess
class TestPreprocessFromYAML:  # pragma: no cover
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
        ]["stages"]["preprocess"]["target_config"]
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
    def test_preprocess(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        stage = (
            PreprocessStageBuilder()
            .encoding()
            .datatypes()
            .newlines()
            .datetime()
            .build()
        )
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.passport, DatasetPassport)
        assert isinstance(target.file, FileSet)
        assert isinstance(target.dataframe, (pd.DataFrame, pd.core.frame.DataFrame))

        logging.info(target.passport)
        logging.info(target.file)
        logging.info(target.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
