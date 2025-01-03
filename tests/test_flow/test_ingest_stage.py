#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_ingest_stage.py                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday January 2nd 2025 06:30:50 pm                                               #
# Modified   : Thursday January 2nd 2025 08:10:10 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.dataset.builder import DatasetPassportBuilder
from discover.asset.dataset.dataset import Dataset
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.flow.dataprep.ingest.builder import IngestStageBuilder

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.ingest
class TestIngestStage:  # pragma: no cover
    # ============================================================================================ #
    def test_build_run(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Construct the stage
        builder = IngestStageBuilder()
        stage = (
            builder.source_passport(source_passport)
            .source_filepath(from_config=True)
            .target_passport(target_passport)
            .encoding()
            .datatypes()
            .newlines()
            .convert_datetime_utc()
            .build()
            .stage
        )

        # Run the stage
        target = stage.run()

        assert isinstance(target, Dataset)
        assert isinstance(target.dataframe, pd.DataFrame)
        logging.info(target.dataframe.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_source_passport(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Test source passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .target_passport(target_passport)
                .encoding()
                .datatypes()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )

        # Test invalid source passport
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(2)
                .source_filepath(from_config=True)
                .target_passport(target_passport)
                .encoding()
                .datatypes()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_target_passport(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )

        # Test target passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .source_passport(source_passport)
                .encoding()
                .datatypes()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )

        # Test invalid target passport
        with pytest.raises(ValueError):
            _ = (
                builder.source_passport(source_passport)
                .source_filepath(from_config=True)
                .target_passport(2)
                .encoding()
                .datatypes()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_encoding_task(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Test target passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .source_passport(source_passport)
                .target_passport(target_passport)
                .datatypes()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_datatypes_task(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Test target passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .source_passport(source_passport)
                .target_passport(target_passport)
                .encoding()
                .newlines()
                .convert_datetime_utc()
                .build()
                .stage
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_newlines_task(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Test target passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .source_passport(source_passport)
                .target_passport(target_passport)
                .encoding()
                .datatypes()
                .convert_datetime_utc()
                .build()
                .stage
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_validate_dt_convert_task(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        # Construct the source passport
        source_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.RAW)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Build target passport
        target_passport = (
            DatasetPassportBuilder()
            .phase(PhaseDef.DATAPREP)
            .stage(DataPrepStageDef.INGEST)
            .parquet()
            .as_pandas()
            .name("reviews")
            .build()
            .passport
        )
        # Test target passport
        builder = IngestStageBuilder()
        with pytest.raises(ValueError):
            _ = (
                builder.source_filepath(from_config=True)
                .source_passport(source_passport)
                .target_passport(target_passport)
                .encoding()
                .datatypes()
                .newlines()
                .build()
                .stage
            )

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
