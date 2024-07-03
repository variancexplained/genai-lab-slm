#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /test/dataset/test_dataset_repo.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday July 2nd 2024 02:11:45 pm                                                   #
# Modified   : Tuesday July 2nd 2024 05:14:05 pm                                                   #
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
from appinsight.infrastructure.persist.repo import DatasetRepo

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


@pytest.mark.repo
class TestDatasetRepo:  # pragma: no cover
    # ============================================================================================ #
    def test_add(self, reviews, caplog):
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
        repo = DatasetRepo()
        dataset = repo.add(dataset=dataset)
        assert isinstance(dataset.oid, int)
        assert len(repo) == 1
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dataset = DatasetRepo().get(1)
        assert isinstance(dataset, Dataset)
        assert dataset.oid == 1
        assert dataset.name == DATASET_NAME
        assert dataset.description == DATASET_DESCRIPTION
        assert dataset.phase == DATASET_PHASE
        assert dataset.stage == DATASET_STAGE
        assert dataset.creator == DATASET_CREATOR
        assert isinstance(dataset.content, pd.DataFrame)
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
    def test_remove(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        repo = DatasetRepo()
        repo.remove(1)
        assert len(repo) == 0
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
