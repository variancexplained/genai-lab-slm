#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_utils/test_cache.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday June 6th 2024 02:45:51 pm                                                  #
# Modified   : Thursday June 6th 2024 02:59:06 pm                                                  #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
import time
from datetime import datetime

import pytest

from appinsight.utils.cache import cachenow

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.cache
class TestCache:  # pragma: no cover
    # ============================================================================================ #
    def test_cache_property(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at \
                {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)

        # ---------------------------------------------------------------------------------------- #
        class ExampleClass:
            @property
            @cachenow(max_size=100)
            def expensive_property(self):
                # Simulate an expensive calculation
                time.sleep(3)
                return 50

            @cachenow(max_size=100)
            def expensive_method(self, value):
                # Simulate an expensive calculation
                time.sleep(3)
                return value * 2

        example = ExampleClass()
        start_time = datetime.now()
        result = example.expensive_property
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        assert duration > 2
        assert result == 50

        example = ExampleClass()
        start_time = datetime.now()
        result = example.expensive_property
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        assert duration < 2
        assert result == 50

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at \
                {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_cache_method(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at \
                {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)

        # ---------------------------------------------------------------------------------------- #
        class ExampleClass:
            @property
            @cachenow(max_size=100)
            def expensive_property(self):
                # Simulate an expensive calculation
                time.sleep(3)
                return 50

            @cachenow(max_size=100)
            def expensive_method(self, value):
                # Simulate an expensive calculation
                time.sleep(3)
                return value * 2

        example = ExampleClass()
        start_time = datetime.now()
        result = example.expensive_method(5)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        assert duration > 2

        example = ExampleClass()
        start_time = datetime.now()
        result = example.expensive_method(5)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        assert duration < 2
        assert result == 10

        example = ExampleClass()
        start_time = datetime.now()
        result = example.expensive_method(10)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        assert duration > 2
        assert result == 20
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} \
                seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
