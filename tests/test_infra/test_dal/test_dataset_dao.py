#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /tests/test_infra/test_dal/test_dataset_dao.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 08:29:47 pm                                                #
# Modified   : Wednesday December 18th 2024 12:07:57 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pytest

from discover.core.flow import PhaseDef, StageDef
from discover.infra.persistence.dal.object.dataset import DatasetDAL

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# mypy: ignore-errors
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


def check_storage_config(a, b):
    assert a.storage_config.compression == b.storage_config.compression
    assert a.storage_config.data_structure == b.storage_config.data_structure
    assert a.storage_config.engine == b.storage_config.engine
    assert (
        a.storage_config.existing_data_behavior
        == b.storage_config.existing_data_behavior
    )
    assert a.storage_config.filepath == b.storage_config.filepath
    assert a.storage_config.index == b.storage_config.index
    assert a.storage_config.mode == b.storage_config.mode
    assert a.storage_config.nlp == b.storage_config.nlp
    assert a.storage_config.parquet_block_size == b.storage_config.parquet_block_size
    assert a.storage_config.partition_cols == b.storage_config.partition_cols
    assert a.storage_config.partitioned == b.storage_config.partitioned
    assert a.storage_config.read_kwargs == b.storage_config.read_kwargs
    assert a.storage_config.row_group_size == b.storage_config.row_group_size
    assert a.storage_config.spark_session_name == b.storage_config.spark_session_name
    assert a.storage_config.write_kwargs == b.storage_config.write_kwargs


@pytest.mark.dataset
@pytest.mark.dataset_dao
@pytest.mark.dao
class TestPandasDatasetDAL:  # pragma: no cover
    # ============================================================================================ #
    def test_setup(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        dao.reset(force=True)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_create_centralized_ds(self, centralized_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        ds = dao.create(dataset=centralized_ds)
        assert dao.exists(id=centralized_ds.id)
        assert isinstance(ds.persisted, datetime)
        assert ds.cost > 0.0

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_centralized_ds(self, centralized_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        ds = dao.read(id=centralized_ds.id)
        assert ds.id == centralized_ds.id
        assert ds.name == centralized_ds.name
        assert ds.description == centralized_ds.description
        assert ds.created == centralized_ds.created
        assert ds.persisted == centralized_ds.persisted
        assert ds.nrows == centralized_ds.nrows
        assert ds.ncols == centralized_ds.ncols
        assert ds.size == centralized_ds.size
        check_storage_config(a=ds, b=centralized_ds)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dataset
@pytest.mark.dataset_dao
@pytest.mark.dao
class TestPandasPartitionedDatasetDAL:  # pragma: no cover
    # ============================================================================================ #
    def test_create_centralized_ds(self, pandas_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        pandas_partitioned_ds.content = None
        pandas_partitioned_ds.persist()
        dao.create(dataset=pandas_partitioned_ds)
        assert dao.exists(id=pandas_partitioned_ds.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_centralized_ds(self, pandas_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        ds = dao.read(id=pandas_partitioned_ds.id)
        assert ds.id == pandas_partitioned_ds.id
        assert ds.name == pandas_partitioned_ds.name
        assert ds.description == pandas_partitioned_ds.description
        assert ds.created == pandas_partitioned_ds.created
        assert ds.persisted == pandas_partitioned_ds.persisted
        assert ds.nrows == pandas_partitioned_ds.nrows
        assert ds.ncols == pandas_partitioned_ds.ncols
        assert ds.size == pandas_partitioned_ds.size
        check_storage_config(a=ds, b=pandas_partitioned_ds)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dataset
@pytest.mark.dataset_dao
@pytest.mark.dao
class TestSparkDatasetDAL:  # pragma: no cover
    # ============================================================================================ #
    def test_create_distributed_ds(self, distributed_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        distributed_ds.content = None
        distributed_ds.persist()
        dao.create(dataset=distributed_ds)
        assert dao.exists(id=distributed_ds.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_distributed_ds(self, distributed_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        ds = dao.read(id=distributed_ds.id)
        assert ds.id == distributed_ds.id
        assert ds.name == distributed_ds.name
        assert ds.description == distributed_ds.description
        assert ds.created == distributed_ds.created
        assert ds.persisted == distributed_ds.persisted
        assert ds.nrows == distributed_ds.nrows
        assert ds.ncols == distributed_ds.ncols
        assert ds.size == distributed_ds.size
        check_storage_config(a=ds, b=distributed_ds)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dataset
@pytest.mark.dataset_dao
@pytest.mark.dao
class TestSparkPartitionedDatasetDAL:  # pragma: no cover
    # ============================================================================================ #
    def test_create_distributed_ds(self, spark_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        spark_partitioned_ds.content = None
        spark_partitioned_ds.persist()
        dao.create(dataset=spark_partitioned_ds)
        assert dao.exists(id=spark_partitioned_ds.id)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_get_distributed_ds(self, spark_partitioned_ds, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        ds = dao.read(id=spark_partitioned_ds.id)
        assert ds.id == spark_partitioned_ds.id
        assert ds.name == spark_partitioned_ds.name
        assert ds.description == spark_partitioned_ds.description
        assert ds.created == spark_partitioned_ds.created
        assert ds.persisted == spark_partitioned_ds.persisted
        assert ds.nrows == spark_partitioned_ds.nrows
        assert ds.ncols == spark_partitioned_ds.ncols
        assert ds.size == spark_partitioned_ds.size
        check_storage_config(a=ds, b=spark_partitioned_ds)
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.dataset
@pytest.mark.dataset_dao
@pytest.mark.dao
class TestDALReading:  # pragma: no cover
    # ============================================================================================ #
    def test_read_all(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        logger.info(dao.read_all)

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read_by_phase(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        logger.info(dao.read_by_phase(phase=PhaseDef.DATAPREP))
        logger.info(dao.read_by_phase(phase=PhaseDef.TRANSFORMATION))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_read_by_stage(self, caplog) -> None:
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        dao = DatasetDAL()
        logger.info(dao.read_by_stage(stage=StageDef.RAW))

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
