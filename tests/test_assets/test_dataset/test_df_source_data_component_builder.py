#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_df_source_data_component_builder.py            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:02:58 am                                               #
# Modified   : Saturday December 28th 2024 06:47:54 pm                                             #
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
from discover.asset.dataset.builder.data import DFSourceDataComponentBuilder

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
INVALID_DF = {"a": 1}


@pytest.mark.dataset
@pytest.mark.builder
class TestDFSourceDataComponentBuilder:  # pragma: no cover
    # ============================================================================================ #
    @pytest.mark.pandas_csv
    def test_pandas_dataframe_builder_csv(
        self, ds_passport, pandas_df, workspace, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        builder = DFSourceDataComponentBuilder(
            passport=ds_passport, workspace=workspace
        )

        # Test normal operation
        data = builder.data(pandas_df).pandas().to_csv().build().data_component

        # Test dtype infer
        data = builder.data(pandas_df).to_csv().build().data_component

        assert isinstance(data.dftype, DFType)
        assert data.dftype == DFType.PANDAS
        assert isinstance(data.filepath, str)
        assert isinstance(data.file_format, FileFormat)
        assert data.file_format == FileFormat.CSV
        assert isinstance(data.data, (pd.DataFrame, pd.core.frame.DataFrame))

        # Test invalid dataframe type
        with pytest.raises(ValueError):
            data = builder.data(pandas_df).spark().to_csv().build().data_component

        with pytest.raises(ValueError):
            data = builder.data(pandas_df).sparknlp().to_csv().build().data_component

        # Test invalid dataframe
        with pytest.raises(TypeError):
            data = builder.data(INVALID_DF).to_csv().build().data_component
        # Test missing dataframe
        with pytest.raises(ValueError):
            data = builder.to_csv().build().data_component
        # Test no methods
        with pytest.raises(TypeError):
            data = builder.build().data_component
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
