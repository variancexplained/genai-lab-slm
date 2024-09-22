#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_domain/test_entities/test_config/test_service_config.py                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 01:07:21 am                                              #
# Modified   : Saturday September 21st 2024 11:45:21 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.element.entity.config.dataset import DatasetConfig
from discover.element.entity.config.service import StageConfig
from discover.element.entity.context.service import ServiceContext
from discover.element.exception.config import InvalidConfigException
from discover.element.value_objects.lifecycle import EDataPrepStage, EPhase

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.config
@pytest.mark.service
class TestStageConfig:  # pragma: no cover
    # ============================================================================================ #
    def test_valid_config(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = StageConfig(
            service_context=ServiceContext(
                phase=EPhase.DATAPREP, estage=EDataPrepStage.DQA
            ),
            source_data_config=DatasetConfig(
                service_context=ServiceContext(
                    phase=EPhase.ANALYSIS, estage=EDataPrepStage.AGGTRICS
                ),
                name="somename",
                estage=EDataPrepStage.DQA,
            ),
            target_data_config=DatasetConfig(
                service_context=ServiceContext(
                    phase=EPhase.ANALYSIS, estage=EDataPrepStage.AGGTRICS
                ),
                name="somename",
                estage=EDataPrepStage.DQA,
            ),
        )
        assert isinstance(config, StageConfig)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_invalid_config(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(InvalidConfigException):
            _ = StageConfig(
                service_context=2, source_data_config=3, target_data_config=3, force=9
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
