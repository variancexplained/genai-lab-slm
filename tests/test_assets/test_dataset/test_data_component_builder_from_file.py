#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_assets/test_dataset/test_data_component_builder_from_file.py            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 10:02:58 am                                               #
# Modified   : Saturday December 28th 2024 09:09:18 pm                                             #
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

from discover.asset.dataset import DFType, FileFormat
from discover.asset.dataset.builder.data import FileSourceDataComponentBuilder

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

# ------------------------------------------------------------------------------------------------ #
FP_PARQUET = "tests/data/reviews"
FP_CSV = "tests/data/reviews.csv"


@pytest.mark.dataset
@pytest.mark.builder
@pytest.mark.filebuilder
class TestFileSourceDataComponentBuilder:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        fp2 = "workspace/test/files/test_dataset-0_dataprep-02_clean-test_dataset_passport.parquet"
        try:
            shutil.rmtree(os.path.dirname(fp2))
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
    @pytest.mark.pandas_csv
    def test_pandas_dataframe_builder_csv(
        self, ds_passport, pandas_df, container, workspace, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        fao = container.repo.fao()
        spark_session_pool = container.spark.session_pool
        # Construct builder
        builder = FileSourceDataComponentBuilder(
            passport=ds_passport,
            workspace=workspace,
            fao=fao,
            spark_session_pool=spark_session_pool,
        )

        # Test normal operation
        data = builder.filepath(FP_CSV).as_pandas().csv().build().data_component
        assert isinstance(data.dftype, DFType)
        assert data.dftype == DFType.PANDAS
        assert isinstance(data.filepath, str)
        assert isinstance(data.file_format, FileFormat)
        assert data.file_format == FileFormat.CSV
        assert isinstance(data.data, (pd.DataFrame, pd.core.frame.DataFrame))
        self.test_setup(caplog=caplog)

        # Test file format infer
        data = builder.filepath(FP_CSV).as_pandas().build().data_component
        assert isinstance(data.dftype, DFType)
        assert data.dftype == DFType.PANDAS
        assert isinstance(data.filepath, str)
        assert isinstance(data.file_format, FileFormat)
        assert data.file_format == FileFormat.CSV
        assert isinstance(data.data, (pd.DataFrame, pd.core.frame.DataFrame))
        self.test_setup(caplog=caplog)

        # Test missing filepath
        with pytest.raises(TypeError):
            data = builder.build().data_component

        # Test missing dataframe type
        with pytest.raises(TypeError):
            data = builder.filepath(FP_CSV).build().data_component

        # Test incompatible file type
        with pytest.raises(ValueError):
            data = builder.filepath(FP_CSV).as_pandas().parquet().build().data_component

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    @pytest.mark.pandas_parquet
    def test_pandas_dataframe_builder_parquet(
        self, ds_passport, pandas_df, container, workspace, caplog
    ) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        fao = container.repo.fao()
        spark_session_pool = container.spark.session_pool
        # Construct builder
        builder = FileSourceDataComponentBuilder(
            passport=ds_passport,
            workspace=workspace,
            fao=fao,
            spark_session_pool=spark_session_pool,
        )

        # Test normal operation
        data = builder.filepath(FP_PARQUET).as_pandas().parquet().build().data_component
        assert isinstance(data.dftype, DFType)
        assert data.dftype == DFType.PANDAS
        assert isinstance(data.filepath, str)
        assert isinstance(data.file_format, FileFormat)
        assert data.file_format == FileFormat.PARQUET
        assert isinstance(data.data, (pd.DataFrame, pd.core.frame.DataFrame))
        self.test_setup(caplog=caplog)

        # Test file format infer
        data = builder.filepath(FP_PARQUET).as_pandas().build().data_component
        assert isinstance(data.dftype, DFType)
        assert data.dftype == DFType.PANDAS
        assert isinstance(data.filepath, str)
        assert isinstance(data.file_format, FileFormat)
        assert data.file_format == FileFormat.PARQUET
        assert isinstance(data.data, (pd.DataFrame, pd.core.frame.DataFrame))
        self.test_setup(caplog=caplog)

        # Test missing filepath
        with pytest.raises(TypeError):
            data = builder.build().data_component

        # Test missing dataframe type
        with pytest.raises(TypeError):
            data = builder.filepath(FP_PARQUET).build().data_component

        # Test incompatible file type
        with pytest.raises(ValueError):
            data = builder.filepath(FP_PARQUET).as_pandas().csv().build().data_component

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
