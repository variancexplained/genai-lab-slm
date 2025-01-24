#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dataprep/test_ingest.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 11:07:32 pm                                             #
# Modified   : Thursday January 23rd 2025 08:52:13 pm                                              #
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
from discover.flow.dataprep.ingest.builder import IngestStageBuilder
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


@pytest.mark.ingest
class TestIngestFromYAML:  # pragma: no cover
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
        ]["stages"]["ingest"]["target_config"]
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
    def test_ingest(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        stage = (
            IngestStageBuilder().encoding().datatypes().newlines().datetime().build()
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
