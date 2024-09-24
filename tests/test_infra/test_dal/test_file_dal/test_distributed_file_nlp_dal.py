#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_dal/test_file_dal/test_distributed_file_nlp_dal.py           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 23rd 2024 08:45:15 pm                                              #
# Modified   : Tuesday September 24th 2024 03:46:46 pm                                             #
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

from discover.infra.dal.file.distributed import DistributedFileSystemNLPDAO

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
@pytest.mark.nlp
class TestDistributedFileSystemNLPDAO:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, spark_storage_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        try:
            shutil.rmtree(os.path.dirname(spark_storage_nlp.filepath))
        except Exception:
            os.remove(spark_storage_nlp.filepath)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_write(self, spark_df, spark_storage_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemNLPDAO()
        dao.create(
            filepath=spark_storage_nlp.filepath,
            data=spark_df,
            **spark_storage_nlp.write_kwargs,
        )
        assert os.path.exists(spark_storage_nlp.filepath)

        df = dao.read(
            filepath=spark_storage_nlp.filepath,
            spark_session_name=spark_storage_nlp.spark_session_name,
            **spark_storage_nlp.read_kwargs,
        )

        logger.info(df.head())
        logger.info(spark_df.head())
        assertDataFrameEqual(df, spark_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, spark_df, spark_storage_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemNLPDAO()
        df = dao.read(
            filepath=spark_storage_nlp.filepath,
            spark_session_name=spark_storage_nlp.spark_session_name,
            **spark_storage_nlp.read_kwargs,
        )

        logger.info(df.head())
        logger.info(spark_df.head())
        assertDataFrameEqual(df, spark_df)
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
class TestDistributedFileSystemDAOPartitionedNLP:  # pragma: no cover

    # ============================================================================================ #
    def test_write(self, spark_df, spark_partitioned_storage_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemNLPDAO()
        dao.create(
            filepath=spark_partitioned_storage_nlp.filepath,
            data=spark_df,
            **spark_partitioned_storage_nlp.write_kwargs,
        )
        assert os.path.isdir(spark_partitioned_storage_nlp.filepath)

        df = dao.read(
            filepath=spark_partitioned_storage_nlp.filepath,
            spark_session_name=spark_partitioned_storage_nlp.spark_session_name,
            **spark_partitioned_storage_nlp.read_kwargs,
        )
        logger.info(df.head())
        logger.info(spark_df.head())
        assertDataFrameEqual(df, spark_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, spark_df, spark_partitioned_storage_nlp, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DistributedFileSystemNLPDAO()
        df = dao.read(
            filepath=spark_partitioned_storage_nlp.filepath,
            spark_session_name=spark_partitioned_storage_nlp.spark_session_name,
            **spark_partitioned_storage_nlp.read_kwargs,
        )
        logger.info(df.head())
        logger.info(spark_df.head())
        assertDataFrameEqual(df, spark_df)
        assert isinstance(df, pyspark.sql.DataFrame)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
