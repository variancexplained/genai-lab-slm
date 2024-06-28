#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_data/prep/quality/test_outliers.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 04:13:06 pm                                                    #
# Modified   : Tuesday June 4th 2024 12:10:40 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from appinsight.data_prep.dqa import DetectOutliersTask

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.quality
@pytest.mark.outliers
class TestDetectOutliersTask:  # pragma: no cover
    # ============================================================================================ #
    def test_outlier_detection(self, data_norm, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = DetectOutliersTask()
        df = task.execute(data=data_norm)
        assert f"{task.PREFIX}vote_sum_outlier" in df.columns
        assert f"{task.PREFIX}vote_count_outlier" in df.columns
        assert f"{task.PREFIX}review_length_outlier" in df.columns
        assert df[f"{task.PREFIX}vote_sum_outlier"].sum() > 4
        assert df[f"{task.PREFIX}vote_count_outlier"].sum() > 4
        assert df[f"{task.PREFIX}review_length_outlier"].sum() > 4

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
