#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_repo/test_dataset_repo.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 08:20:27 pm                                                #
# Modified   : Wednesday October 9th 2024 12:57:18 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
from datetime import datetime

import pytest

from discover.core.data_structure import DataStructure
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.dataset.build import DatasetBuilder
from discover.infra.repo.dataset import DatasetRepo

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.repo
@pytest.mark.dataset_repo
class TestDatasetRepoPandas:  # pragma: no cover
    # ============================================================================================ #
    def test_add_get_remove_exists(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        content = pandas_df
        builder = DatasetBuilder()
        dataset = (
            builder.name("test_pandas_repo")
            .phase(PhaseDef.TRANSFORMATION)
            .stage(DataPrepStageDef.CLEAN)
            .data_structure(DataStructure.PANDAS)
            .not_partitioned()
            .content(content)
            .build()
        )
        repo = DatasetRepo()
        repo.add(dataset=dataset)
        assert repo.exists(id=dataset.id)
        # ---------------------------------------------------------------------------------------- #
        ds2 = repo.get(id=dataset.id)
        assert dataset == ds2
        assert os.path.exists(dataset.storage_config.filepath)
        # ---------------------------------------------------------------------------------------- #
        repo.remove(id=dataset.id)
        assert not repo.exists(id=dataset.id)
        assert not os.path.exists(dataset.storage_config.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.repo
@pytest.mark.dataset_repo
class TestDatasetRepoSpark:  # pragma: no cover
    # ============================================================================================ #
    def test_add_get_remove_exists(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        content = spark_df
        builder = DatasetBuilder()
        dataset = (
            builder.name("test_spark_dataset_repo")
            .phase(PhaseDef.TRANSFORMATION)
            .stage(DataPrepStageDef.CLEAN)
            .data_structure(DataStructure.SPARK)
            .partitioned()
            .content(content)
            .build()
        )
        repo = DatasetRepo()
        repo.add(dataset=dataset)
        assert repo.exists(id=dataset.id)
        # ---------------------------------------------------------------------------------------- #
        ds2 = repo.get(id=dataset.id)
        assert dataset == ds2
        assert os.path.exists(dataset.storage_config.filepath)
        # ---------------------------------------------------------------------------------------- #
        repo.remove(id=dataset.id)
        assert not repo.exists(id=dataset.id)
        assert not os.path.exists(dataset.storage_config.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
