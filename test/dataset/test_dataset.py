#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /test/dataset/test_dataset.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 02:11:45 pm                                                   #
# Modified   : Tuesday July 2nd 2024 05:12:15 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from appinsight.domain.dataset import Dataset

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"
# ------------------------------------------------------------------------------------------------ #
DATASET_NAME = "test_dataset"
DATASET_DESCRIPTION = "test dataset description"
DATASET_PHASE = "PREP"
DATASET_STAGE = "DQA"
DATASET_CREATOR = "TestDataset"


@pytest.mark.dataset
class TestDataset:  # pragma: no cover
    # ============================================================================================ #
    def test_dataset(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = Dataset(
            name=DATASET_NAME,
            description=DATASET_DESCRIPTION,
            phase=DATASET_PHASE,
            stage=DATASET_STAGE,
            creator=DATASET_CREATOR,
            created=datetime.now(),
            content=reviews,
        )
        assert dataset.oid is None
        assert dataset.name == DATASET_NAME
        assert dataset.description == DATASET_DESCRIPTION
        assert dataset.phase == DATASET_PHASE
        assert dataset.stage == DATASET_STAGE
        assert dataset.creator == DATASET_CREATOR
        assert isinstance(dataset.created, datetime)
        assert isinstance(dataset.nrows, int)
        assert isinstance(dataset.ncols, int)
        assert isinstance(dataset.size, np.int64)
        assert isinstance(dataset.cols, list)
        logger.info(dataset)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_name(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(ValueError):
            _ = Dataset(
                name=None,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created=datetime.now(),
                content=reviews,
            )

        with pytest.raises(ValueError):
            _ = Dataset(
                name="",
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created=datetime.now(),
                content=reviews,
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_content(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(TypeError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created=datetime.now(),
            )

        with pytest.raises(ValueError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created=datetime.now(),
                content=pd.DataFrame(),
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_phase(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(ValueError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase="bogus",
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created=datetime.now(),
                content=reviews,
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_stage(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(ValueError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage="BOGUS",
                creator=DATASET_CREATOR,
                created=datetime.now(),
                content=reviews,
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_creator(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(ValueError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=None,
                created=datetime.now(),
                content=reviews,
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_dataset_validate_created(self, reviews, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #

        with pytest.raises(TypeError):
            _ = Dataset(
                name=DATASET_NAME,
                description=DATASET_DESCRIPTION,
                phase=DATASET_PHASE,
                stage=DATASET_STAGE,
                creator=DATASET_CREATOR,
                created="bogus",
                content=reviews,
            )
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
