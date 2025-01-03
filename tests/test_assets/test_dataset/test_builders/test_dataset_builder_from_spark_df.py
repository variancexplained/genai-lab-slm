#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_builders/test_dataset_builder_from_spark_df.py #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday December 29th 2024 01:22:15 pm                                               #
# Modified   : Friday January 3rd 2025 06:03:21 am                                                 #
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
from pyspark.sql import DataFrame

from discover.asset.dataset.builder import DatasetBuilder
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetPassport
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
NAME = "test_build_spark_dataset_to_csv"


# ------------------------------------------------------------------------------------------------ #
@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.dsbs
class TestDatasetBuilderSparkCSV:  # pragma: no cover
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
    def test_build_dataset_to_csv(
        self, workspace, spark_df, spark, ds_passport, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = (
            DatasetBuilder(workspace=workspace)
            .passport(ds_passport)
            .from_dataframe(spark_df)
            .spark(spark)
            .to_csv()
            .build()
            .dataset
        )

        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.asset_id)
        logger.info(dataset.file_format.value)
        logger.info(dataset.filepath)
        assert isinstance(dataset.dataframe, DataFrame)

        # Evaluate File Info
        logger.info(dataset.file_meta)
        assert isinstance(dataset.file_meta, FileMeta)
        assert isinstance(dataset.file_meta.filepath, str)
        assert os.path.exists(dataset.file_meta.filepath)
        assert isinstance(dataset.file_meta.file_type, str)
        assert dataset.file_meta.file_type == "csv"
        assert dataset.file_meta.isdir is True
        assert dataset.file_meta.file_count > 1
        assert isinstance(dataset.file_meta.created, datetime)
        assert isinstance(dataset.file_meta.accessed, datetime)
        assert isinstance(dataset.file_meta.modified, datetime)
        assert isinstance(dataset.file_meta.size, int)
        assert dataset.size > 0

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
    def test_validate_passport_missing(
        self, workspace, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .from_dataframe(spark_df)
                .spark(spark)
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport_invalid(
        self, workspace, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(2)
                .from_dataframe(spark_df)
                .spark(spark)
                .to_csv()
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_missing(self, workspace, spark, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .to_csv()
                .spark(spark)
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_invalid(
        self, workspace, ds_passport, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .from_dataframe(2)
                .to_csv()
                .spark(spark)
                .build()
                .dataset
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_spark(
        self, workspace, ds_passport, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .from_dataframe(spark_df)
                .to_csv()
                .build()
                .dataset
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.dsbs
class TestDatasetBuilderSparkPARQUET:  # pragma: no cover
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
    def test_build_dataset_to_parquet(
        self, workspace, spark_df, spark, ds_passport, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = (
            DatasetBuilder(workspace=workspace)
            .passport(ds_passport)
            .from_dataframe(spark_df)
            .to_parquet()
            .spark(spark)
            .build()
            .dataset
        )

        assert isinstance(dataset, Dataset)

        # Evaluate Passport Component
        logger.info(dataset.passport)
        assert isinstance(dataset.passport, DatasetPassport)

        # Evaluate Data Component
        logger.info(dataset.asset_id)
        logger.info(dataset.file_format.value)
        logger.info(dataset.filepath)
        assert isinstance(dataset.dataframe, DataFrame)

        # Evaluate File Info
        logger.info(dataset.file_meta)
        assert isinstance(dataset.file_meta, FileMeta)
        assert isinstance(dataset.file_meta.filepath, str)
        assert os.path.exists(dataset.file_meta.filepath)
        assert isinstance(dataset.file_meta.file_type, str)
        assert dataset.file_meta.file_type == "parquet"
        assert dataset.file_meta.isdir is True
        assert dataset.file_meta.file_count > 1
        assert isinstance(dataset.file_meta.created, datetime)
        assert isinstance(dataset.file_meta.accessed, datetime)
        assert isinstance(dataset.file_meta.modified, datetime)
        assert isinstance(dataset.file_meta.size, int)
        assert dataset.size > 0

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
    def test_validate_passport_missing(
        self, workspace, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .from_dataframe(spark_df)
                .spark(spark)
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_passport_invalid(
        self, workspace, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(2)
                .from_dataframe(spark_df)
                .spark(spark)
                .to_parquet()
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_missing(self, workspace, spark, ds_passport, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .to_parquet()
                .spark(spark)
                .build()
                .dataset
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_df_invalid(
        self, workspace, ds_passport, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .from_dataframe(2)
                .to_parquet()
                .spark(spark)
                .build()
                .dataset
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_spark(
        self, workspace, ds_passport, spark_df, spark, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder(workspace=workspace)
                .passport(ds_passport)
                .from_dataframe(spark_df)
                .to_parquet()
                .build()
                .dataset
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
