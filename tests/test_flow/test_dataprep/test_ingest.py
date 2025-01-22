#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_flow/test_dataprep/test_ingest.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday January 21st 2025 08:23:44 pm                                               #
# Modified   : Tuesday January 21st 2025 08:45:48 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.asset.dataset.builder import DatasetBuilder
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.utils.file.fileset import FileFormat

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"

FP = "tests/data/dirty_reviews.parquet"


@pytest.mark.ingest
class TestIngest:  # pragma: no cover
    # ============================================================================================ #
    def test_create_build_raw_dataset(self, container, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = (
            DatasetBuilder()
            .from_file(filepath=FP)
            .name("test_ingest_dataset")
            .phase(PhaseDef.DATAPREP)
            .stage(StageDef.RAW)
            .creator("TestIngest")
            .as_pandas()
            .to_parquet()
            .build()
            .dataset
        )

        # Test Passport
        logging.info(dataset.passport)
        assert isinstance(dataset.passport.asset_id, str)
        assert dataset.passport.asset_type == "dataset"
        assert isinstance(dataset.passport.phase, PhaseDef)
        assert dataset.passport.phase == PhaseDef.DATAPREP
        assert dataset.passport.stage == StageDef.RAW
        assert dataset.passport.name == "test_ingest_dataset"
        assert isinstance(dataset.passport.description, str)
        assert dataset.passport.creator == "TestIngest"
        assert isinstance(dataset.passport.created, datetime)
        assert isinstance(dataset.passport.filepath, str)
        assert isinstance(dataset.passport.file_format, FileFormat)
        assert dataset.passport.dftype == DFType.PANDAS
        assert isinstance(dataset.passport.version, str)
        # Print components
        logging.info(dataset.dataframer.info)
        logging.info(dataset.dataframer.summary)
        logging.info(dataset.file)

        # persist
        repo = container.io.dataset_repo()
        repo.add(asset=dataset)
        ds2 = repo.get(asset_id=dataset.asset_id)
        assert ds2 == dataset

        # Print Data
        logging.info(ds2.dataframer.dataframe.head())

        # Print File  Information
        logging.info(ds2.file)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_build_validation(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        with pytest.raises(ValueError):
            _ = DatasetBuilder().from_file(filepath=FP).build().dataset

        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder()
                .from_file(filepath=FP)
                .name("test_ingest_dataset")
                .build()
                .dataset
            )

        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder()
                .from_file(filepath=FP)
                .name("test_ingest_dataset")
                .phase(PhaseDef.DATAPREP)
                .build()
                .dataset
            )

        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder()
                .from_file(filepath=FP)
                .name("test_ingest_dataset")
                .phase(PhaseDef.DATAPREP)
                .stage(StageDef.RAW)
                .build()
                .dataset
            )

        with pytest.raises(ValueError):
            _ = (
                DatasetBuilder()
                .from_file(filepath=FP)
                .name("test_ingest_dataset")
                .phase(PhaseDef.DATAPREP)
                .stage(StageDef.RAW)
                .creator("TestIngest")
                .as_pandas()
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
