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
# Modified   : Tuesday September 24th 2024 02:23:55 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
import shutil
from datetime import datetime

import pandas as pd
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


@pytest.mark.central
@pytest.mark.file
@pytest.mark.dal
class TestCentralizedFileSystemDAO:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, pandas_storage, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        try:
            shutil.rmtree(os.path.dirname(pandas_storage.filepath))
        except Exception:
            os.remove(pandas_storage.filepath)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_write(self, pandas_df, pandas_storage, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        dao.create(
            filepath=pandas_storage.filepath,
            data=pandas_df,
            **pandas_storage.write_kwargs,
        )
        assert os.path.isfile(pandas_storage.filepath)

        df = dao.read(filepath=pandas_storage.filepath, **pandas_storage.read_kwargs)

        logger.info(df.head())
        logger.info(pandas_df.head())
        assert df.equals(pandas_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, pandas_df, pandas_storage, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        df = dao.read(filepath=pandas_storage.filepath, **pandas_storage.read_kwargs)

        logger.info(df.head())
        logger.info(pandas_df.head())
        assert df.equals(pandas_df)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.central
@pytest.mark.file
@pytest.mark.dal
class TestCentralizedFileSystemDAOPartitioned:  # pragma: no cover

    # ============================================================================================ #
    def test_write(self, pandas_df, pandas_partitioned_storage, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        dao.create(
            filepath=pandas_partitioned_storage.filepath,
            data=pandas_df,
            **pandas_partitioned_storage.write_kwargs,
        )
        assert os.path.isdir(pandas_partitioned_storage.filepath)

        df = dao.read(
            filepath=pandas_partitioned_storage.filepath,
            **pandas_partitioned_storage.read_kwargs,
        )
        logger.info(df.head())
        logger.info(pandas_df.head())
        assert df.equals(pandas_df)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read(self, pandas_df, pandas_partitioned_storage, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = CentralizedFileSystemDAO()
        df = dao.read(
            filepath=pandas_partitioned_storage.filepath,
            **pandas_partitioned_storage.read_kwargs,
        )
        logger.info(df.head())
        logger.info(pandas_df.head())
        assert df.equals(pandas_df)
        assert isinstance(df, (pd.DataFrame, pd.core.frame.DataFrame))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
