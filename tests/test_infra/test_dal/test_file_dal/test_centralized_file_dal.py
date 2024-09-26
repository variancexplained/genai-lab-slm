#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_dal/test_file_dal/test_centralized_file_dal.py               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday September 23rd 2024 08:45:15 pm                                              #
# Modified   : Thursday September 26th 2024 03:48:09 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
import shutil
from datetime import datetime

import pytest

from discover.infra.dal.file.centralized import CentralizedFileSystemDAO

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.centralized
@pytest.mark.file
@pytest.mark.dal
class TestCentralizedFileSystemDAO:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, pandas_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        try:
            shutil.rmtree(os.path.dirname(pandas_ds.storage_config.filepath))
        except Exception:
            os.remove(pandas_ds.storage_config.filepath)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_write(self, pandas_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        dao.create(
            filepath=pandas_ds.storage_config.filepath,
            data=pandas_ds.content,
            **pandas_ds.storage_config.write_kwargs,
        )
        assert os.path.exists(pandas_ds.storage_config.filepath)

        df = dao.read(
            filepath=pandas_ds.storage_config.filepath,
            **pandas_ds.storage_config.read_kwargs,
        )

        logger.info(df.head())
        logger.info(pandas_ds.content.head())
        assert df.equals(pandas_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, pandas_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        df = dao.read(
            filepath=pandas_ds.storage_config.filepath,
            **pandas_ds.storage_config.read_kwargs,
        )

        logger.info(df.head())
        logger.info(pandas_ds.content.head())
        assert df.equals(pandas_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.centralized
@pytest.mark.file
@pytest.mark.dal
@pytest.mark.nlp
class TestCentralizedFileSystemDAOPartitioned:  # pragma: no cover

    # ============================================================================================ #
    def test_write(self, pandas_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        dao.create(
            filepath=pandas_partitioned_ds.storage_config.filepath,
            data=pandas_partitioned_ds.content,
            **pandas_partitioned_ds.storage_config.write_kwargs,
        )
        assert os.path.isdir(pandas_partitioned_ds.storage_config.filepath)

        df = dao.read(
            filepath=pandas_partitioned_ds.storage_config.filepath,
            **pandas_partitioned_ds.storage_config.read_kwargs,
        )
        logger.info(df.head())
        logger.info(pandas_partitioned_ds.content.head())
        assert df.equals(pandas_partitioned_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, pandas_df, pandas_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        df = dao.read(
            filepath=pandas_partitioned_ds.storage_config.filepath,
            **pandas_partitioned_ds.storage_config.read_kwargs,
        )
        logger.info(df.head())
        logger.info(pandas_partitioned_ds.content.head())
        assert df.equals(pandas_partitioned_ds.content)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
