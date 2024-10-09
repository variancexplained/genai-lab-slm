#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/repo/dataset.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 07:31:47 pm                                                #
# Modified   : Wednesday October 9th 2024 01:02:46 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
from typing import Optional, Union

import pandas as pd
import pyspark

from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef
from discover.element.base.store import Repo
from discover.element.dataset.define import Dataset
from discover.infra.config.reader import ConfigReader
from discover.infra.dal.db.dataset import DatasetDAO
from discover.infra.dal.file.centralized import CentralizedFileSystemDAO as CFSDAO
from discover.infra.dal.file.distributed import DistributedFileSystemDAO as DFSDAO


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    def __init__(
        self,
        config_reader_cls: type[ConfigReader] = ConfigReader,
        dataset_dao_cls: type[DatasetDAO] = DatasetDAO,
        cfs_dao_cls: type[CFSDAO] = CFSDAO,
        dfs_dao_cls: type[DFSDAO] = DFSDAO,
    ) -> None:
        self._config_reader = config_reader_cls()
        self._dataset_dao = dataset_dao_cls()
        self._cfs_dao = cfs_dao_cls()
        self._dfs_dao = dfs_dao_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, dataset: Dataset) -> None:
        if self._dataset_dao.exists(id=dataset.id):
            msg = f"Dataset id: {dataset.id} already exists."
            self._logger.error(msg)
            raise FileExistsError(msg)

        # Write contents to file
        self._write_file(dataset=dataset)
        # Prepare Dataset for serialization
        dataset.content = None
        # Save dataset metadata and storage config
        self._dataset_dao.create(dataset=dataset)

    def get(self, id: int) -> Optional[Dataset]:
        if self._dataset_dao.exists(id=id):
            try:
                dataset = self._dataset_dao.read(id=id)
                dataset.content = self._read_file(dataset=dataset)
                return dataset
            except Exception as e:
                msg = f"Unknown exception occurred while retrieving dataset id: {id}.\n{e}"
                self._logger.error(msg)
                raise FileNotFoundError(msg)
        else:
            msg = f"No dataset exists with id: {id}"
            self._logger.warning(msg)

    def list_all(self) -> pd.DataFrame:
        return self._dataset_dao.read_all()

    def list_by_phase(self, phase: PhaseDef) -> pd.DataFrame:
        return self._dataset_dao.read_by_phase(phase=phase)

    def list_by_stage(self, stage: StageDef) -> pd.DataFrame:
        return self._dataset_dao.read_by_stage(stage=stage)

    def remove(self, id: int) -> None:
        dataset = self._dataset_dao.read(id=id)
        self._cfs_dao.delete(filepath=dataset.storage_config.filepath)
        self._dataset_dao.delete(id=id)

    def exists(self, id: int) -> bool:
        return self._dataset_dao.exists(id=id)

    def _read_file(
        self, dataset: Dataset
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:

        # Read content from file system
        if dataset.storage_config.data_structure == DataStructure.PANDAS:
            return self._read_centralized_file(dataset=dataset)
        else:
            return self._read_distributed_file(dataset=dataset)

    def _read_centralized_file(self, dataset: Dataset) -> pd.DataFrame:
        return self._cfs_dao.read(filepath=dataset.storage_config.filepath)

    def _read_distributed_file(self, dataset: Dataset) -> pyspark.sql.DataFrame:
        spark_config = self._config_reader.get_config(section="spark", namespace=False)
        session_name = spark_config[dataset.phase.value]
        return self._dfs_dao.read(
            filepath=dataset.storage_config.filepath, spark_session_name=session_name
        )

    def _write_file(self, dataset: Dataset) -> None:
        if dataset.storage_config.data_structure == DataStructure.PANDAS:
            self._write_centralized_file(dataset=dataset)
        else:
            self._write_distributed_file(dataset=dataset)

    def _write_centralized_file(self, dataset: Dataset) -> None:
        self._cfs_dao._write(
            filepath=dataset.storage_config.filepath,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )

    def _write_distributed_file(self, dataset: Dataset) -> None:
        self._dfs_dao._write(
            filepath=dataset.storage_config.filepath,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )
