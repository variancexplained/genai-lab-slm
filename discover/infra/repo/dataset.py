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
# Modified   : Friday October 11th 2024 07:09:06 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
import os
from typing import Optional, Union

import pandas as pd
import pyspark

from discover.core.flow import PhaseDef, StageDef
from discover.element.dataset import Dataset
from discover.element.repo import Repo
from discover.infra.config.reader import ConfigReader
from discover.infra.dal.dao.dataset import DatasetDAO
from discover.infra.dal.fao.centralized import CentralizedFileSystemDAO as CFSDAO
from discover.infra.dal.fao.distributed import DistributedFileSystemDAO as DFSDAO
from discover.infra.dal.fao.exception import FileIOException
from discover.infra.dal.fao.filepath import FilePathService
from discover.infra.repo.config import CentralizedDatasetStorageConfig
from discover.infra.repo.exception import DatasetExistsError

# ------------------------------------------------------------------------------------------------ #
filepath_service = FilePathService()


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """
    Repository class for managing dataset storage and retrieval operations.

    This class provides methods to add, retrieve, list, and remove datasets, as well as
    read and write datasets to centralized (Pandas) or distributed (PySpark) file systems.
    It leverages DAOs (Data Access Objects) for interacting with underlying storage mechanisms.

    Attributes:
        _config_reader (ConfigReader): Instance to read configuration settings.
        _dataset_dao (DatasetDAO): Data Access Object for dataset metadata.
        _cfs_dao (CFSDAO): Data Access Object for centralized file storage (e.g., local filesystem).
        _dfs_dao (DFSDAO): Data Access Object for distributed file storage (e.g., HDFS).
        _logger (logging.Logger): Logger for logging operations and errors.

    Methods:
        add(dataset: Dataset) -> None:
            Adds a new dataset to the storage system.

        get(id: int) -> Optional[Dataset]:
            Retrieves a dataset by its ID.

        list_all() -> pd.DataFrame:
            Returns a DataFrame containing all datasets' metadata.

        list_by_phase(phase: PhaseDef) -> pd.DataFrame:
            Lists datasets filtered by the specified phase.

        list_by_stage(stage: StageDef) -> pd.DataFrame:
            Lists datasets filtered by the specified stage.

        remove(id: int) -> None:
            Removes a dataset and its associated content by its ID.

        exists(id: int) -> bool:
            Checks if a dataset exists by its ID.

    Private Methods:
        _read_file(dataset: Dataset) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
            Reads a dataset's content based on its storage configuration.

        _read_centralized_file(dataset: Dataset) -> pd.DataFrame:
            Reads a dataset's content from a centralized file system (e.g., local filesystem).

        _read_distributed_file(dataset: Dataset) -> pyspark.sql.DataFrame:
            Reads a dataset's content from a distributed file system (e.g., HDFS).

        _write_file(dataset: Dataset) -> None:
            Writes a dataset's content based on its storage configuration.

        _write_centralized_file(dataset: Dataset) -> None:
            Writes a dataset's content to a centralized file system.

        _write_distributed_file(dataset: Dataset) -> None:
            Writes a dataset's content to a distributed file system.
    """

    def __init__(
        self,
        config_reader_cls: type[ConfigReader] = ConfigReader,
        dataset_dao_cls: type[DatasetDAO] = DatasetDAO,
        cfs_dao_cls: type[CFSDAO] = CFSDAO,
        dfs_dao_cls: type[DFSDAO] = DFSDAO,
    ) -> None:
        """
        Initializes the DatasetRepo with instances of configuration reader and DAOs.

        Args:
            config_reader_cls (type[ConfigReader]): Class for reading configuration settings.
            dataset_dao_cls (type[DatasetDAO]): Class for dataset metadata management.
            cfs_dao_cls (type[CFSDAO]): Class for centralized file storage operations.
            dfs_dao_cls (type[DFSDAO]): Class for distributed file storage operations.
        """
        self._config_reader = config_reader_cls()
        self._dataset_dao = dataset_dao_cls()
        self._cfs_dao = cfs_dao_cls()
        self._dfs_dao = dfs_dao_cls()
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def add(self, dataset: Dataset) -> None:
        """
        Adds a new dataset to the repository. Raises an error if the dataset ID already exists.

        Args:
            dataset (Dataset): The dataset object to be added.

        Raises:
            FileExistsError: If a dataset with the given ID already exists.
        """
        # Ensure dataset doesn't already exist
        self._validate_add(dataset=dataset)

        try:
            # Write contents to file
            self._write_file(dataset=dataset)
        except FileIOException as e:
        # Save dataset metadata and storage config
        self._dataset_dao.create(dataset=dataset)

    def get(self, name: str) -> Optional[Dataset]:
        """
        Retrieves a dataset by its ID.

        Args:
            name (str): The name of the dataset to retrieve.

        Returns:
            Optional[Dataset]: The dataset object if found; otherwise, None.

        Raises:
            FileNotFoundError: If an error occurs while reading the dataset.
        """
        try:
            dataset = self._dataset_dao.read(name=name)
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
        """
        Lists all datasets' metadata.

        Returns:
            pd.DataFrame: A DataFrame containing metadata for all datasets.
        """
        return self._dataset_dao.read_all()

    def list_by_phase(self, phase: PhaseDef) -> pd.DataFrame:
        """
        Lists datasets filtered by the specified phase.

        Args:
            phase (PhaseDef): The phase to filter datasets by.

        Returns:
            pd.DataFrame: A DataFrame containing metadata for the filtered datasets.
        """
        return self._dataset_dao.read_by_phase(phase=phase)

    def remove(self, name: str, ignore_errors: bool = True) -> None:
        """
        Removes a dataset and its associated content by its ID.

        Args:
            name (str): The name of the dataset to be removed.
            ignore_errors (bool): If True, errors encountered will be ignored.
                 Otherwise, errors will be propagated to the caller.
        """
        dataset = self._dataset_dao.read(name=name)
        filepath = self._format_filepath(
            phase=dataset.phase,
            stage=dataset.stage,
            partitioned=dataset.storage_config.partitioned,
        )
        self._cfs_dao.delete(filepath=filepath)
        self._dataset_dao.delete(name=name)

    def exists(self, name: str) -> bool:
        """
        Checks if a dataset exists by its ID.

        Args:
            name (str): The name to check for existence.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        return self._dataset_dao.exists(name=name)

    def _read_file(
        self, dataset: Dataset
    ) -> Union[pd.DataFrame, pyspark.sql.DataFrame]:
        """
        Reads a dataset's content based on its storage configuration.

        Args:
            dataset (Dataset): The dataset to read.

        Returns:
            Union[pd.DataFrame, pyspark.sql.DataFrame]: The dataset's content.
        """
        filepath = self._format_filepath(
            phase=dataset.phase,
            stage=dataset.stage,
            partitioned=dataset.storage_config.partitioned,
        )
        if isinstance(dataset.storage_config, CentralizedDatasetStorageConfig):
            return self._read_centralized_file(dataset=dataset, filepath=filepath)
        else:
            return self._read_distributed_file(dataset=dataset, filepath=filepath)

    def _read_centralized_file(self, dataset: Dataset, filepath: str) -> pd.DataFrame:
        """
        Reads a dataset's content from a centralized file system.

        Args:
            dataset (Dataset): The dataset to read.

        Returns:
            pd.DataFrame: The content of the dataset.
        """
        return self._cfs_dao.read(
            filepath=filepath, **dataset.storage_config.read_kwargs
        )

    def _read_distributed_file(
        self, dataset: Dataset, filepath: str
    ) -> pyspark.sql.DataFrame:
        """
        Reads a dataset's content from a distributed file system.

        Args:
            dataset (Dataset): The dataset to read.

        Returns:
            pyspark.sql.DataFrame: The content of the dataset.
        """

        return self._dfs_dao.read(filepath=filepath, nlp=dataset.storage_config.nlp)

    def _write_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content based on its storage configuration.

        Args:
            dataset (Dataset): The dataset to write.
        """
        filepath = self._format_filepath(
            phase=dataset.phase,
            stage=dataset.stage,
            partitioned=dataset.storage_config.partitioned,
        )

        if isinstance(dataset.storage_config, CentralizedDatasetStorageConfig):
            self._write_centralized_file(dataset=dataset, filepath=filepath)
        else:
            self._write_distributed_file(dataset=dataset, filepath=filepath)

    def _write_centralized_file(self, dataset: Dataset, filepath: str) -> None:
        """
        Writes a dataset's content to a centralized file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._cfs_dao._write(
            filepath=filepath,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )

    def _write_distributed_file(self, dataset: Dataset, filepath: str) -> None:
        """
        Writes a dataset's content to a distributed file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._dfs_dao._write(
            filepath=filepath,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )

    def _validate_add(self, dataset: Dataset) -> None:
        """Ensures dataset object and file doesn't already exist"""
        if self.exists(name=dataset.name):
            msg = f"Unable to add dataset {dataset.name} as it already exists."
            self._logger.error(msg)
            raise DatasetExistsError(msg)

    def _format_filepath(
        self, phase: PhaseDef, stage: StageDef, partitioned: bool = True
    ) -> str:
        """Dynamic generates the dataset filepath"""
        filename = f"{phase.value}_{stage.value}"
        filename = filename + ".parquet" if not partitioned else filename
        filepath = os.path.join(phase.directory, filename)
        return filepath_service.get_filepath(filepath)
