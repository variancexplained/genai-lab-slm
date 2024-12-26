#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_spark_dataset.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 10:41:20 pm                                               #
# Modified   : Thursday December 26th 2024 05:34:39 am                                             #
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
from pyspark.sql import DataFrame

from discover.asset.dataset import DatasetFactory
from discover.core.data_structure import DataFrameStructureEnum
from discover.core.file import FileFormat
from discover.core.flow import DataPrepStageEnum, PhaseEnum

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.factory
@pytest.mark.dataset
@pytest.mark.spark
class TestSparkDataset:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        try:
            shutil.rmtree("workspace/test/files/")
            shutil.rmtree("workspace/test/assets/")
        except FileNotFoundError:
            pass
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_parquet_to_spark(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        FP = "tests/data/reviews"
        factory = DatasetFactory()
        dataset = factory.from_parquet_file(
            filepath=FP,
            phase=PhaseEnum.DATAPREP,
            stage=DataPrepStageEnum.CLEAN,
            name="test_from_parquet_to_spark",
            dataframe_structure=DataFrameStructureEnum.SPARK,
        )
        assert dataset.dataframe_structure == DataFrameStructureEnum.SPARK
        assert dataset.file_format == FileFormat.PARQUET
        assert dataset.asset_id == "dataset-dataprep-clean-test_from_parquet_to_spark"
        assert (
            dataset.filepath
            == "workspace/test/files/dataset-dataprep-clean-test_from_parquet_to_spark.parquet"
        )
        assert dataset.phase == PhaseEnum.DATAPREP
        assert dataset.stage == DataPrepStageEnum.CLEAN
        assert isinstance(dataset.to_pandas(), pd.DataFrame)
        assert isinstance(dataset.to_spark(), DataFrame)
        assert isinstance(dataset.to_sparknlp(), DataFrame)

        # Test registration in workspace
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=dataset.asset_id)
        assert ds == dataset

        # Confirm file persistence
        assert os.path.exists(dataset.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_csv_to_spark(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        FP = "tests/data/reviews.csv"
        factory = DatasetFactory()
        dataset = factory.from_csv_file(
            filepath=FP,
            phase=PhaseEnum.DATAPREP,
            stage=DataPrepStageEnum.CLEAN,
            name="test_from_csv_to_spark",
            dataframe_structure=DataFrameStructureEnum.SPARK,
        )
        assert dataset.dataframe_structure == DataFrameStructureEnum.SPARK
        assert dataset.file_format == FileFormat.CSV
        assert dataset.asset_id == "dataset-dataprep-clean-test_from_csv_to_spark"
        assert (
            dataset.filepath
            == "workspace/test/files/dataset-dataprep-clean-test_from_csv_to_spark.csv"
        )
        assert dataset.phase == PhaseEnum.DATAPREP
        assert dataset.stage == DataPrepStageEnum.CLEAN
        assert isinstance(dataset.to_pandas(), pd.DataFrame)
        assert isinstance(dataset.to_spark(), DataFrame)
        assert isinstance(dataset.to_sparknlp(), DataFrame)

        # Test registration in workspace
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=dataset.asset_id)
        assert ds == dataset

        # Confirm file persistence
        assert os.path.exists(dataset.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_spark_to_parquet(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        asset_id = "dataset-dataprep-clean-test_from_parquet_to_spark"
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=asset_id)
        df = ds.to_spark()

        factory = DatasetFactory()
        dataset = factory.from_spark_dataframe(
            data=df,
            phase=PhaseEnum.DATAPREP,
            stage=DataPrepStageEnum.CLEAN,
            name="test_from_spark_to_parquet",
            file_format=FileFormat.PARQUET,
        )
        assert dataset.dataframe_structure == DataFrameStructureEnum.SPARK
        assert dataset.file_format == FileFormat.PARQUET
        assert dataset.asset_id == "dataset-dataprep-clean-test_from_spark_to_parquet"
        assert (
            dataset.filepath
            == "workspace/test/files/dataset-dataprep-clean-test_from_spark_to_parquet.parquet"
        )
        assert dataset.phase == PhaseEnum.DATAPREP
        assert dataset.stage == DataPrepStageEnum.CLEAN
        assert isinstance(dataset.to_pandas(), pd.DataFrame)
        assert isinstance(dataset.to_spark(), DataFrame)
        assert isinstance(dataset.to_sparknlp(), DataFrame)

        # Test registration in workspace
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=dataset.asset_id)
        assert ds == dataset

        # Confirm file persistence
        assert os.path.exists(dataset.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_from_spark_to_csv(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        asset_id = "dataset-dataprep-clean-test_from_csv_to_spark"
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=asset_id)
        df = ds.to_spark()

        factory = DatasetFactory()
        dataset = factory.from_spark_dataframe(
            data=df,
            phase=PhaseEnum.DATAPREP,
            stage=DataPrepStageEnum.CLEAN,
            name="test_from_spark_to_csv",
            file_format=FileFormat.CSV,
        )
        assert dataset.dataframe_structure == DataFrameStructureEnum.SPARK
        assert dataset.file_format == FileFormat.CSV
        assert dataset.asset_id == "dataset-dataprep-clean-test_from_spark_to_csv"
        assert (
            dataset.filepath
            == "workspace/test/files/dataset-dataprep-clean-test_from_spark_to_csv.csv"
        )
        assert dataset.phase == PhaseEnum.DATAPREP
        assert dataset.stage == DataPrepStageEnum.CLEAN
        assert isinstance(dataset.to_pandas(), pd.DataFrame)
        assert isinstance(dataset.to_spark(), DataFrame)
        assert isinstance(dataset.to_sparknlp(), DataFrame)

        # Test registration in workspace
        workspace_service = container.workspace.service()
        ds = workspace_service.dataset_repo.get(asset_id=dataset.asset_id)
        assert ds == dataset

        # Confirm file persistence
        assert os.path.exists(dataset.filepath)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
