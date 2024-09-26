#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_dal/test_file_dal/test_distributed_file_dal.py               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 23rd 2024 08:45:15 pm                                              #
# Modified   : Thursday September 26th 2024 03:39:53 pm                                            #
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

from discover.infra.dal.file.distributed import DistributedFileSystemDAO

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.distributed
@pytest.mark.file
@pytest.mark.dal
class TestDistributedFileSystemDAO:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        try:
            shutil.rmtree(os.path.dirname(spark_ds.storage_config.filepath))
        except Exception:
            os.remove(spark_ds.storage_config.filepath)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_write(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemDAO()
        dao.create(
            filepath=spark_ds.storage_config.filepath,
            data=spark_ds.content,
            **spark_ds.storage_config.write_kwargs,
        )
        assert os.path.exists(spark_ds.storage_config.filepath)

        df = dao.read(
            filepath=spark_ds.storage_config.filepath,
            spark_session_name=spark_ds.storage_config.spark_session_name,
            **spark_ds.storage_config.read_kwargs,
        )

        logger.info(df.head())
        logger.info(spark_ds.content.head())
        assertDataFrameEqual(df, spark_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, spark_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemDAO()
        df = dao.read(
            filepath=spark_ds.storage_config.filepath,
            spark_session_name=spark_ds.storage_config.spark_session_name,
            **spark_ds.storage_config.read_kwargs,
        )

        logger.info(df.head())
        logger.info(spark_ds.content.head())
        assertDataFrameEqual(df, spark_ds.content)
        assert isinstance(df, pyspark.sql.DataFrame)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.distributed
@pytest.mark.file
@pytest.mark.dal
@pytest.mark.nlp
class TestDistributedFileSystemDAOPartitioned:  # pragma: no cover

    # ============================================================================================ #
    def test_write(self, spark_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemDAO()
        dao.create(
            filepath=spark_partitioned_ds.storage_config.filepath,
            data=spark_partitioned_ds.content,
            **spark_partitioned_ds.storage_config.write_kwargs,
        )
        assert os.path.isdir(spark_partitioned_ds.storage_config.filepath)

        df = dao.read(
            filepath=spark_partitioned_ds.storage_config.filepath,
            spark_session_name=spark_partitioned_ds.storage_config.spark_session_name,
            **spark_partitioned_ds.storage_config.read_kwargs,
        )
        logger.info(df.head())
        logger.info(spark_partitioned_ds.content.head())
        assertDataFrameEqual(df, spark_partitioned_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, spark_df, spark_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemDAO()
        df = dao.read(
            filepath=spark_partitioned_ds.storage_config.filepath,
            spark_session_name=spark_partitioned_ds.storage_config.spark_session_name,
            **spark_partitioned_ds.storage_config.read_kwargs,
        )
        logger.info(df.head())
        logger.info(spark_partitioned_ds.content.head())
        assertDataFrameEqual(df, spark_partitioned_ds.content)
        assert isinstance(df, pyspark.sql.DataFrame)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
