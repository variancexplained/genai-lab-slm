#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_elements/test_dataset/test_dataset_and_dao.py                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 25th 2024 03:46:36 pm                                           #
# Modified   : Saturday October 12th 2024 10:46:58 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.infra.dal.dao.dataset import DatasetDAO

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dataset
@pytest.mark.dao
class TestDatasetDAO:  # pragma: no cover
    # ============================================================================================ #
    def test_pandas_dataset(self, centralized_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = centralized_ds

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(name=dataset.name)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        with pytest.raises(FileExistsError):
            dao.create(dataset=dataset)

        # Now delete
        dao.delete(dataset.name)
        assert not dao.exists(dataset.name)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pandas_partitioned_dataset(self, centralized_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = centralized_ds

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(name=dataset.name)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        with pytest.raises(FileExistsError):
            dao.create(dataset=dataset)

        # Now delete
        dao.delete(dataset.name)
        assert not dao.exists(dataset.name)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_dataset(self, distributed_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = distributed_ds

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(name=dataset.name)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        with pytest.raises(FileExistsError):
            dao.create(dataset=dataset)

        # Now delete
        dao.delete(dataset.name)
        assert not dao.exists(dataset.name)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_partitioned_dataset(self, spark_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = spark_partitioned_ds

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(name=dataset.name)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        with pytest.raises(FileExistsError):
            dao.create(dataset=dataset)

        # Now delete
        dao.delete(dataset.name)
        assert not dao.exists(dataset.name)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_partitioned_dataset_nlp(self, distributed_ds_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = distributed_ds_nlp

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(name=dataset.name)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        with pytest.raises(FileExistsError):
            dao.create(dataset=dataset)

        # Now delete
        dao.delete(dataset.name)
        assert not dao.exists(dataset.name)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
