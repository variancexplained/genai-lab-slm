#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_builders/test_dataset_builder_from_file.py     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 29th 2024 01:22:15 pm                                               #
# Modified   : Wednesday January 1st 2025 02:00:39 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import os
import shutil
import time
from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import DataFrame

from discover.asset.dataset.base import DatasetComponent
from discover.asset.dataset.builder.data import DataComponentBuilderFromFile
from discover.asset.dataset.builder.dataset import DatasetBuilder
from discover.asset.dataset.component.identity import DatasetPassport
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import PhaseDef, TestStageDef
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
# ------------------------------------------------------------------------------------------------ #
PHASE = PhaseDef.TESTING
STAGE = TestStageDef.SMOKE_TEST
NAME = "test_build_pandas_dataset_from_csv"

# ------------------------------------------------------------------------------------------------ #
CSV_FILEPATH = "tests/data/reviews.csv"
PARQUET_FILEPATH = "tests/data/reviews"
BOGUS_FILEPATH = "tests/data/bogus"


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.dsbuilder_file
class TestDatasetBuilderPandas:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, workspace, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = workspace.dataset_repo
        repo.reset()
        try:
            shutil.rmtree(workspace.files)
        except Exception:
            pass

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_build_pandas_dataset_from_csv(
        self, workspace, ds_passport, pandas_df, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        data = (
            DataComponentBuilderFromFile()
            .passport(ds_passport)
            .source_filepath(CSV_FILEPATH)
            .from_csv()
            .to_pandas()
            .build()
            .data_component
        )

        # Evaluate Data Component Befoe Build
        logger.info(data.dataframe.head())
        dataset = DatasetBuilder().passport(ds_passport).data(data).build().dataset
        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.data.asset_id)
        logger.info(dataset.data.file_format.value)
        logger.info(dataset.data.filepath)
        assert isinstance(dataset.data, DatasetComponent)
        assert isinstance(
            dataset.data.dataframe, (pd.DataFrame, pd.core.frame.DataFrame)
        )

        # Evaluate File Info
        logger.info(dataset.data.file_meta)
        assert isinstance(dataset.data.file_meta, FileMeta)
        assert isinstance(dataset.data.file_meta.filepath, str)
        assert os.path.exists(dataset.data.file_meta.filepath)
        assert isinstance(dataset.data.file_meta.file_type, str)
        assert dataset.data.file_meta.file_type == "csv"
        assert dataset.data.file_meta.isdir is False
        assert dataset.data.file_meta.file_count == 1
        assert isinstance(dataset.data.file_meta.created, datetime)
        assert isinstance(dataset.data.file_meta.accessed, datetime)
        assert isinstance(dataset.data.file_meta.modified, datetime)
        assert isinstance(dataset.data.file_meta.size, int)
        assert dataset.data.size > 0

        # Check the data from repository
        repo = workspace.dataset_repo
        ds = repo.get(asset_id=ds_passport.asset_id)
        assert ds == dataset

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_build_pandas_dataset_from_parquet(
        self, workspace, ds_passport, pandas_df, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        data = (
            DataComponentBuilderFromFile()
            .passport(ds_passport)
            .source_filepath(PARQUET_FILEPATH)
            .from_parquet()
            .to_pandas()
            .build()
            .data_component
        )

        # Evaluate Data Component Befoe Build
        logger.info(data.dataframe.head())
        dataset = DatasetBuilder().passport(ds_passport).data(data).build().dataset
        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.data.asset_id)
        logger.info(dataset.data.file_format.value)
        logger.info(dataset.data.filepath)
        assert isinstance(dataset.data, DatasetComponent)
        assert isinstance(
            dataset.data.dataframe, (pd.DataFrame, pd.core.frame.DataFrame)
        )

        # Evaluate File Info
        logger.info(dataset.data.file_meta)
        assert isinstance(dataset.data.file_meta, FileMeta)
        assert isinstance(dataset.data.file_meta.filepath, str)
        assert os.path.exists(dataset.data.file_meta.filepath)
        assert isinstance(dataset.data.file_meta.file_type, str)
        assert dataset.data.file_meta.file_type == "parquet"
        assert dataset.data.file_meta.isdir is True
        assert dataset.data.file_meta.file_count > 1
        assert isinstance(dataset.data.file_meta.created, datetime)
        assert isinstance(dataset.data.file_meta.accessed, datetime)
        assert isinstance(dataset.data.file_meta.modified, datetime)
        assert isinstance(dataset.data.file_meta.size, int)
        assert dataset.data.size > 0

        # Check the data from repository
        repo = workspace.dataset_repo
        ds = repo.get(asset_id=ds_passport.asset_id)
        assert ds == dataset

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.dsbuilder_file
class TestDatasetBuilderSpark:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, workspace, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = workspace.dataset_repo
        repo.reset()
        try:
            shutil.rmtree(workspace.files)
        except Exception:
            pass

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_build_spark_dataset_from_csv(
        self, workspace, ds_passport, spark, spark_df, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        data = (
            DataComponentBuilderFromFile()
            .passport(ds_passport)
            .source_filepath(CSV_FILEPATH)
            .from_csv()
            .to_spark()
            .build()
            .data_component
        )

        # Evaluate Data Component Befoe Build
        logger.info(data.dataframe.head())
        dataset = DatasetBuilder().passport(ds_passport).data(data).build().dataset
        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.data.asset_id)
        logger.info(dataset.data.file_format.value)
        logger.info(dataset.data.filepath)
        assert isinstance(dataset.data, DatasetComponent)
        assert isinstance(dataset.data.dataframe, DataFrame)
        # Evaluate File Info
        logger.info(dataset.data.file_meta)
        assert isinstance(dataset.data.file_meta, FileMeta)
        assert isinstance(dataset.data.file_meta.filepath, str)
        assert os.path.exists(dataset.data.file_meta.filepath)
        assert isinstance(dataset.data.file_meta.file_type, str)
        assert dataset.data.file_meta.file_type == "csv"
        assert dataset.data.file_meta.isdir is True
        assert dataset.data.file_meta.file_count > 1
        assert isinstance(dataset.data.file_meta.created, datetime)
        assert isinstance(dataset.data.file_meta.accessed, datetime)
        assert isinstance(dataset.data.file_meta.modified, datetime)
        assert isinstance(dataset.data.file_meta.size, int)
        assert dataset.data.size > 0

        # Check the data from repository
        repo = workspace.dataset_repo
        ds = repo.get(asset_id=ds_passport.asset_id, spark=spark)
        assert ds == dataset

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_build_spark_dataset_from_parquet(
        self, workspace, ds_passport, spark, spark_df, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        data = (
            DataComponentBuilderFromFile()
            .passport(ds_passport)
            .source_filepath(PARQUET_FILEPATH)
            .from_parquet()
            .to_spark()
            .build()
            .data_component
        )

        # Evaluate Data Component Befoe Build
        logger.info(data.dataframe.head())
        dataset = DatasetBuilder().passport(ds_passport).data(data).build().dataset
        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.data.asset_id)
        logger.info(dataset.data.file_format.value)
        logger.info(dataset.data.filepath)
        assert isinstance(dataset.data, DatasetComponent)
        assert isinstance(dataset.data.dataframe, DataFrame)

        # Evaluate File Info
        logger.info(dataset.data.file_meta)
        assert isinstance(dataset.data.file_meta, FileMeta)
        assert isinstance(dataset.data.file_meta.filepath, str)
        assert os.path.exists(dataset.data.file_meta.filepath)
        assert isinstance(dataset.data.file_meta.file_type, str)
        assert dataset.data.file_meta.file_type == "parquet"
        assert dataset.data.file_meta.isdir is True
        assert dataset.data.file_meta.file_count > 1
        assert isinstance(dataset.data.file_meta.created, datetime)
        assert isinstance(dataset.data.file_meta.accessed, datetime)
        assert isinstance(dataset.data.file_meta.modified, datetime)
        assert isinstance(dataset.data.file_meta.size, int)
        assert dataset.data.size > 0

        # Check the data from repository
        repo = workspace.dataset_repo
        ds = repo.get(asset_id=ds_passport.asset_id, spark=spark)
        assert ds == dataset

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_teardown(self, workspace, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        repo = workspace.dataset_repo
        repo.reset()

        if os.path.exists(workspace.files):

            residuals = len(os.listdir(workspace.files))
            if residuals > 0:
                logging.info(
                    f"There are {residuals} residual files left. We'll clean these up manually."
                )
                time.sleep(2)
            try:
                shutil.rmtree(workspace.files)
            except Exception:
                pass
            if os.path.exists(workspace.files):
                residuals = len(os.listdir(workspace.files))
                if residuals > 0:
                    logging.info(
                        f"Still!!  There are {residuals} residual files left. We'll clean these with brute force!!."
                    )
                    time.sleep(2)
                    try:
                        shutil.rmtree(workspace.files)
                    except Exception:
                        pass

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
