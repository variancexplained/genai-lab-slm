#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_builders/test_dataset_builder.py               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 29th 2024 01:22:15 pm                                               #
# Modified   : Monday December 30th 2024 06:32:57 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import shutil
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.base.atype import AssetType
from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import DataComponentBuilderFromDataFrame
from discover.asset.dataset.builder.dataset import DatasetBuilder
from discover.asset.dataset.builder.identity import DatasetPassportBuilder
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import DataPrepStageDef, PhaseDef, StageDef

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"
# ------------------------------------------------------------------------------------------------ #
PHASE = PhaseDef.DATAPREP
STAGE = DataPrepStageDef.DQV
NAME = "test_build_pandas_dataset_to_csv"


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.datasetbuilder
class TestDatasetBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, workspace, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = workspace.dataset_repo
        repo.reset()
        try:
            shutil.rmtree(workspace.files)
        except Exception:
            pass

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_build_pandas_dataset_to_csv(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        passport = (
            DatasetPassportBuilder()
            .phase(PHASE)
            .stage(STAGE)
            .name(NAME)
            .build()
            .passport
        )
        data = (
            DataComponentBuilderFromDataFrame()
            .passport(passport)
            .source(pandas_df)
            .as_pandas()
            .to_csv()
            .build()
            .data
        )
        dataset = DatasetBuilder().passport(passport).data(data).build().dataset

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)
        assert isinstance(dataset.passport.asset_type, AssetType)
        assert isinstance(dataset.passport.asset_id, str)
        assert isinstance(dataset.passport.phase, PhaseDef)
        assert dataset.passport.phase == PHASE
        assert isinstance(dataset.passport.stage, StageDef)
        assert dataset.passport.stage == STAGE
        assert NAME in dataset.passport.asset_id
        assert isinstance(dataset.passport.created, datetime)

        # Evaluate Data Component
        assert isinstance(dataset, Dataset)
        assert dataset.data.dftype == DFType.PANDAS
        assert isinstance(dataset.data.filepath, str)
        assert NAME in dataset.data.filepath
        assert dataset.data.file_format == FileFormat.CSV
        assert isinstance(dataset.data.as_df(), pd.DataFrame)
        assert dataset.data.size > 0
        assert isinstance(dataset.data.accessed, str)

        # Check the repository

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
