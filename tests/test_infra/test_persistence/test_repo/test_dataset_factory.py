#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_persistence/test_repo/test_dataset_factory.py                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday December 18th 2024 08:34:14 pm                                            #
# Modified   : Thursday December 19th 2024 10:49:10 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
from datetime import datetime

import pytest

from discover.assets.data.factory import DatasetFactory
from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
PARQUET_FILEPATH = "tests/data/reviews"
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"
# ------------------------------------------------------------------------------------------------ #

# ------------------------------------------------------------------------------------------------ #
PHASE = PhaseDef.DATAPREP
STAGE = StageDef.INGEST


@pytest.mark.dataset
@pytest.mark.factory
class TestDatasetFactory:  # pragma: no cover
    # ============================================================================================ #
    def test_from_parquet(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        name = inspect.stack()[0][3]
        factory = DatasetFactory()
        ds = factory.from_parquet_file(
            phase=PHASE,
            stage=STAGE,
            name=name,
            filepath=PARQUET_FILEPATH,
            data_structure=DataStructure.PANDAS,
        )
        assert ds.phase == PHASE
        assert ds.stage == STAGE
        assert ds.name == name
        assert ds.asset_id == f"dataset-test-{PHASE.value}-{STAGE.value}-{name}"

        # Validate Dataset existence
        ds_repo = container.object_persistence.dataset_repo()
        assert ds_repo.exists(asset_id=ds.asset_id)

        # Validate Fileset existence
        fs_repo = container.fileset_persistence.fileset_repo()
        fp = fs_repo.get_filepath(asset_id=ds.asset_id)
        assert fs_repo.exists(asset_id=ds.asset_id)
        assert os.path.exists(fp)

        # Clean up
        ds_repo.remove(asset_id=ds.asset_id)
        fs_repo.remove(asset_id=ds.asset_id)
        assert not fs_repo.exists(asset_id=ds.asset_id)
        assert not ds_repo.exists(asset_id=ds.asset_id)
        assert not os.path.exists(fs_repo.get_filepath(asset_id=ds.asset_id))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_pandas(self, container, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        name = inspect.stack()[0][3]
        factory = DatasetFactory()
        ds = factory.from_pandas(
            phase=PHASE,
            stage=STAGE,
            name=name,
            data=pandas_df,
            data_structure=DataStructure.PANDAS,
        )
        assert ds.phase == PHASE
        assert ds.stage == STAGE
        assert ds.name == name
        assert ds.asset_id == f"dataset-test-{PHASE.value}-{STAGE.value}-{name}"

        # Validate Dataset existence
        repo = container.object_persistence.dataset_repo()
        assert repo.exists(asset_id=ds.asset_id)

        # Clean up
        repo.remove(asset_id=ds.asset_id)
        assert not repo.exists(asset_id=ds.asset_id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_spark(self, container, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        name = inspect.stack()[0][3]

        factory = DatasetFactory()
        ds = factory.from_pandas(
            phase=PHASE,
            stage=STAGE,
            name=name,
            data=spark_df,
            data_structure=DataStructure.SPARK,
        )
        assert ds.phase == PHASE
        assert ds.stage == STAGE
        assert ds.name == name
        assert ds.asset_id == f"dataset-test-{PHASE.value}-{STAGE.value}-{name}"

        # Validate Dataset existence
        repo = container.object_persistence.dataset_repo()
        assert repo.exists(asset_id=ds.asset_id)

        # Clean up
        repo.remove(asset_id=ds.asset_id)
        assert not repo.exists(asset_id=ds.asset_id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_teardown(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
