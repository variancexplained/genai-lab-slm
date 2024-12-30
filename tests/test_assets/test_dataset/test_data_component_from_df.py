#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_data_component_from_df.py                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 30th 2024 06:16:36 pm                                               #
# Modified   : Monday December 30th 2024 06:35:24 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import DataComponentBuilderFromDataFrame
from discover.asset.dataset.component.identity import DatasetPassport

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.data
@pytest.mark.builder
@pytest.mark.dataset
class TestDatasetBuilderFromDataFrame:  # pragma: no cover
    # ============================================================================================ #
    def test_builder_df(self, ds_passport, pandas_df, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DataComponentBuilderFromDataFrame()
        dc = (
            builder.data(pandas_df)
            .passport(ds_passport)
            .to_csv()
            .build()
            .data_component
        )

        assert isinstance(dc.passport, DatasetPassport)
        assert isinstance(dc.dftype, DFType)
        assert dc.dftype == DFType.PANDAS
        assert isinstance(dc.filepath, str)
        assert dc.file_format == FileFormat.CSV
        assert isinstance(dc.data, (pd.DataFrame, pd.core.frame.DataFrame))
        assert dc.file_meta is None

        logging.info(dc)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
