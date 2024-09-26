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
# Modified   : Wednesday September 25th 2024 10:32:18 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.element.dataset.build import DatasetBuilder
from discover.element.dataset.define import Dataset
from discover.infra.dal.db.dataset import DatasetDAO

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
    def test_pandas_dataset(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.content(pandas_df)
            .name("test_dataset")
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQA)
            .build()
        )
        assert isinstance(dataset, Dataset)

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(id=dataset.id)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        # Now delete
        dao.delete(dataset.id)
        assert not dao.exists(dataset.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pandas_partitioned_dataset(self, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.content(pandas_df)
            .name("test_dataset")
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQA)
            .build()
        )
        assert isinstance(dataset, Dataset)

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(id=dataset.id)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        # Now delete
        dao.delete(dataset.id)
        assert not dao.exists(dataset.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_dataset(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.content(spark_df)
            .name("test_dataset")
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQA)
            .build()
        )
        assert isinstance(dataset, Dataset)

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(id=dataset.id)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        # Now delete
        dao.delete(dataset.id)
        assert not dao.exists(dataset.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_partitioned_dataset(self, spark_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DatasetBuilder()
        dataset = (
            builder.content(spark_df)
            .name("test_dataset")
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.DQA)
            .build()
        )
        assert isinstance(dataset, Dataset)

        # Persist in KVS
        dao = DatasetDAO()
        dao.create(dataset=dataset)
        ds2 = dao.read(id=dataset.id)

        logging.info(dataset)
        logging.info(ds2)

        logging.info(dataset.content.head())
        logging.info(ds2.content.head())
        assert ds2 == dataset

        # Now delete
        dao.delete(dataset.id)
        assert not dao.exists(dataset.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
