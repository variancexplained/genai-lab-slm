#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_identity.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 08:21:27 pm                                               #
# Modified   : Friday December 27th 2024 11:14:09 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.asset.core import AssetType
from discover.asset.dataset.builder.identity import DatasetPassportBuilder
from discover.asset.dataset.component.identity import DatasetPassport
from discover.core.flow import DataPrepStageDef, PhaseDef

# ------------------------------------------------------------------------------------------------ #
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


@pytest.mark.passport
@pytest.mark.dataset
class TestDatasetPassport:  # pragma: no cover
    # ============================================================================================ #
    def test_builder(self, dataset_builder, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetPassportBuilder(dataset_builder=dataset_builder)
        passport = (
            builder.phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.CLEAN)
            .name("test_dataset_passport_builder")
            .build()
            .passport
        )
        assert isinstance(passport, DatasetPassport)
        assert isinstance(passport.asset_id, str)
        assert isinstance(passport.asset_type, AssetType)
        assert passport.phase == PhaseDef.DATAPREP
        assert passport.stage == DataPrepStageDef.CLEAN
        assert passport.name == "test_dataset_passport_builder"
        assert isinstance(passport.description, str)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validation(self, dataset_builder, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetPassportBuilder(dataset_builder=dataset_builder)

        with pytest.raises(ValueError):
            _ = builder.build().passport

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
