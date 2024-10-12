#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_repo/test_dataset_repo_distributed.py                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 25th 2024 03:46:36 pm                                           #
# Modified   : Saturday October 12th 2024 03:51:43 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
import shutil
from datetime import datetime

import pyspark
import pytest
from pyspark.testing import assertDataFrameEqual

from discover.core.flow import PhaseDef, StageDef
from discover.infra.repo.dataset import DatasetRepo
from discover.infra.repo.exception import (
    DatasetExistsError,
    DatasetIntegrityError,
    DatasetNotFoundError,
    DatasetRemovalError,
)

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
def validate_dataset(dataset):
    assert isinstance(dataset.phase, PhaseDef)
    assert isinstance(dataset.stage, StageDef)
    assert isinstance(dataset.content, pyspark.sql.DataFrame)
    assert isinstance(dataset.created, datetime)
    assert os.path.exists(dataset.storage_location)


# ------------------------------------------------------------------------------------------------ #
def remove_dataset_file(dataset):
    if os.path.exists(dataset.storage_location):
        try:
            shutil.rmtree(dataset.storage_location)
        except Exception:
            os.remove(dataset.storage_location)


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.repo
@pytest.mark.repo_dist
class TestDatasetRepoDistributedAdd:  # pragma: no cover
    # ============================================================================================ #
    def test_add_dataset(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_ds
        repo = DatasetRepo()
        ds = repo.add(dataset=dataset)
        assert repo.exists(dataset.name)
        validate_dataset(ds)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_add_dataset_exists_error(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_ds
        repo = DatasetRepo()
        repo.add(dataset=dataset)
        with pytest.raises(DatasetExistsError):
            repo.add(dataset=dataset)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.repo
@pytest.mark.repo_dist
class TestDatasetRepoDistributedGet:  # pragma: no cover
    # ============================================================================================ #
    def test_get_dataset(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_ds
        repo = DatasetRepo()
        repo.add(dataset=dataset)
        ds2 = repo.get(name=dataset.name)
        validate_dataset(ds2)
        assert dataset == ds2
        assert ds2.phase == dataset.phase
        assert ds2.stage == dataset.stage
        assertDataFrameEqual(ds2.content, dataset.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_dataset_does_not_exist_error(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = DatasetRepo()
        with pytest.raises(DatasetNotFoundError):
            repo.get(name="bogus_dataset_name")
        with pytest.raises(DatasetNotFoundError):
            repo.get(name="dataprep_bogus")

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_dataset_data_integrity_error(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_ds
        repo = DatasetRepo()
        ds = repo.add(dataset=dataset)
        remove_dataset_file(ds)
        with pytest.raises(DatasetIntegrityError):
            repo.get(name=ds.name)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.repo
@pytest.mark.repo_dist
class TestDatasetRepoDistributedRemove:  # pragma: no cover
    # ============================================================================================ #
    def test_remove_dataset(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_ds
        repo = DatasetRepo()
        repo.add(dataset=dataset)
        repo.remove(name=dataset.name)
        assert not repo.exists(name=dataset.name)
        assert not os.path.exists(dataset.storage_location)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_remove_dataset_exceptions(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = DatasetRepo()
        with pytest.raises(DatasetRemovalError):
            repo.remove(name="bogus")

        # Should not raise exception
        repo.remove(name="bogus", ignore_errors=True)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
