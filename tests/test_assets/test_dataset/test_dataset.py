#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_dataset.py                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 2nd 2025 11:16:12 am                                               #
# Modified   : Thursday January 16th 2025 04:31:13 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import shutil
from datetime import datetime

import pytest

from discover.asset.dataset.identity import DatasetPassport
from discover.core.dtypes import DFType
from discover.core.file import FileFormat
from discover.infra.utils.file.info import FileMeta

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.dstest
class TestDataset:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, workspace, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = workspace.dataset_repo
        repo.reset()
        shutil.rmtree(workspace.files)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pandas_csv_dataset(self, workspace, dataset_pandas_csv, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = dataset_pandas_csv
        assert isinstance(dataset.asset_id, str)
        assert isinstance(dataset.name, str)
        assert isinstance(dataset.description, str)
        assert isinstance(dataset.version, str)
        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.passport, DatasetPassport)
        assert isinstance(dataset.dftype, DFType)
        assert dataset.dftype == DFType.PANDAS
        assert dataset.file_format == FileFormat.CSV
        assert dataset.size > 0
        assert isinstance(dataset.file, FileMeta)
        assert isinstance(dataset.file.created, datetime)
        assert isinstance(dataset.file.accessed, datetime)
        assert isinstance(dataset.file.created, datetime)

        # Avoid file already exists errors.
        repo = workspace.dataset_repo
        repo.reset()

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_pandas_parquet_dataset(
        self, workspace, dataset_pandas_parquet, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = dataset_pandas_parquet
        assert isinstance(dataset.asset_id, str)
        assert isinstance(dataset.name, str)
        assert isinstance(dataset.description, str)
        assert isinstance(dataset.version, str)
        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.passport, DatasetPassport)
        assert isinstance(dataset.dftype, DFType)
        assert dataset.dftype == DFType.PANDAS
        assert dataset.file_format == FileFormat.PARQUET
        assert dataset.size > 0
        assert isinstance(dataset.file, FileMeta)
        assert isinstance(dataset.file.created, datetime)
        assert isinstance(dataset.file.accessed, datetime)
        assert isinstance(dataset.file.created, datetime)

        # Avoid file already exists errors.
        repo = workspace.dataset_repo
        repo.reset()
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_csv_dataset(self, workspace, dataset_spark_csv, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = dataset_spark_csv
        assert isinstance(dataset.asset_id, str)
        assert isinstance(dataset.name, str)
        assert isinstance(dataset.description, str)
        assert isinstance(dataset.version, str)
        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.passport, DatasetPassport)
        assert isinstance(dataset.dftype, DFType)
        assert dataset.dftype == DFType.SPARK
        assert dataset.file_format == FileFormat.CSV
        assert dataset.size > 0
        assert isinstance(dataset.file, FileMeta)
        assert isinstance(dataset.file.created, datetime)
        assert isinstance(dataset.file.accessed, datetime)
        assert isinstance(dataset.file.created, datetime)

        # Avoid file already exists errors.
        repo = workspace.dataset_repo
        repo.reset()
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_spark_parquet_dataset(
        self, workspace, dataset_spark_parquet, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = dataset_spark_parquet
        assert isinstance(dataset.asset_id, str)
        assert isinstance(dataset.name, str)
        assert isinstance(dataset.description, str)
        assert isinstance(dataset.version, str)
        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.passport, DatasetPassport)
        assert isinstance(dataset.dftype, DFType)
        assert dataset.dftype == DFType.SPARK
        assert dataset.file_format == FileFormat.PARQUET
        assert dataset.size > 0
        assert isinstance(dataset.file, FileMeta)
        assert isinstance(dataset.file.created, datetime)
        assert isinstance(dataset.file.accessed, datetime)
        assert isinstance(dataset.file.created, datetime)

        # Avoid file already exists errors.
        repo = workspace.dataset_repo
        repo.reset()
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
