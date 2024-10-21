#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_orchestration/test_data_prep/test_dqa.py                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 18th 2024 01:27:37 am                                                #
# Modified   : Monday October 21st 2024 12:06:22 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.assets.idgen import AssetIDGen
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.data_prep.stage import DataPrepStage
from discover.infra.config.flow import FlowConfigReader

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dataprep
@pytest.mark.dqa
class TestDQA:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, norm_asset_id, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        dqa_asset_id = AssetIDGen.get_asset_id(
            name="review",
            asset_type="dataset",
            phase=PhaseDef.DATAPREP,
            stage=DataPrepStageDef.DQA,
        )

        repo = container.repo.dataset_repo()
        assert repo.exists(asset_id=norm_asset_id)
        repo.remove(asset_id=dqa_asset_id, ignore_errors=True)
        assert not repo.exists(asset_id=dqa_asset_id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        reader = FlowConfigReader()
        config = reader.get_config("phases", namespace=False)
        stage_config = config["dataprep"]["stages"][2]

        # Build Stage
        stage = DataPrepStage.build(stage_config=stage_config)
        asset_id = stage.run()
        # Confirm existence
        repo = container.repo.dataset_repo()
        assert repo.exists(asset_id=asset_id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_endpoint_exists(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        reader = FlowConfigReader()
        config = reader.get_config("phases", namespace=False)
        stage_config = config["dataprep"]["stages"][2]

        # Build Stage
        stage = DataPrepStage.build(stage_config=stage_config)
        _ = stage.run()

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dqa_endpoint_exists_override(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        reader = FlowConfigReader()
        config = reader.get_config("phases", namespace=False)
        stage_config = config["dataprep"]["stages"][2]

        # Build Stage
        stage = DataPrepStage.build(stage_config=stage_config, force=True)
        _ = stage.run()

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
