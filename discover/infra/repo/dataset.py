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
# Modified   : Saturday October 12th 2024 01:56:43 am                                              #
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
from discover.infra.dal.dao.exception import ObjectNotFoundError
from discover.infra.dal.fao.centralized import CentralizedFileSystemDAO as CFSDAO
from discover.infra.dal.fao.distributed import DistributedFileSystemDAO as DFSDAO
from discover.infra.dal.fao.filepath import FilePathService
from discover.infra.repo.config import CentralizedDatasetStorageConfig
from discover.infra.repo.exception import (
    DatasetCreationError,
    DatasetExistsError,
    DatasetIntegrityError,
    DatasetIOError,
    DatasetNotFoundError,
    DatasetRemovalError,
)

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

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET CREATION METHODS                                         #
    # -------------------------------------------------------------------------------------------- #
    def add(self, dataset: Dataset) -> Optional[Dataset]:
        """
        Adds a new dataset to the repository. Raises an error if the dataset ID already exists.

        Args:
            dataset (Dataset): The dataset object to be added.

        Returns:
            dataset (Dataset): The dataset object with storage location added.
        Raises:
            FileExistsError: If a dataset with the given ID already exists.
        """
        # Ensure dataset doesn't already exist
        self._validate_add(dataset=dataset)

        # Set the storage location on the dataset object
        dataset.storage_location = self._format_filepath(
            phase=dataset.phase,
            stage=dataset.stage,
            partitioned=dataset.storage_config.partitioned,
        )

        # Save dataset contents to file.
        try:
            self._write_file(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred adding dataset {dataset.name} to the repository."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

        # Save dataset object (metadata and config) to object storage
        try:
            self._dataset_dao.create(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred while saving dataset {dataset.name} to object storage. Rolling back file persistence."

            # Rollback the file write to maintain consistency.
            try:
                self._remove_dataset_file_by_filepath(filepath=dataset.storage_location)
            except Exception as e:
                fileio_msg = f"Exception occurred while rolling back dataset {dataset.name} persistence."
                self._logger.exception(fileio_msg)
                raise DatasetIOError(fileio_msg, e) from e

            # Raise the original exception
            raise DatasetCreationError(msg, e) from e

        return dataset

    # -------------------------------------------------------------------------------------------- #
    def _validate_add(self, dataset: Dataset) -> None:
        """Ensures dataset object and file doesn't already exist"""
        if self.exists(name=dataset.name):
            msg = f"Unable to add dataset {dataset.name} as it already exists."
            self._logger.error(msg)
            raise DatasetExistsError(msg)

    # -------------------------------------------------------------------------------------------- #
    def _write_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content based on its storage configuration.

        Args:
            dataset (Dataset): The dataset to write.
        """
        if isinstance(dataset.storage_config, CentralizedDatasetStorageConfig):
            self._write_centralized_file(dataset=dataset)
        else:
            self._write_distributed_file(dataset=dataset)

    # -------------------------------------------------------------------------------------------- #
    def _write_centralized_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content to a centralized file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._cfs_dao._write(
            filepath=dataset.storage_location,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )

    # -------------------------------------------------------------------------------------------- #
    def _write_distributed_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content to a distributed file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._dfs_dao._write(
            filepath=dataset.storage_location,
            data=dataset.content,
            **dataset.storage_config.write_kwargs,
        )

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET RETRIEVAL METHODS                                        #
    # -------------------------------------------------------------------------------------------- #
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
        # Step 1: Obtain the dataset object containing metadata and config.
        try:
            dataset = self._dataset_dao.read(name=name)
        except ObjectNotFoundError:
            msg = f"Dataset {name} does not exist."
            self._logger.exception(msg)
            raise DatasetNotFoundError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading the dataset {name} object."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

        # Step 2: Get the dataset contents from file, add to dataset object and return
        try:
            dataset.content = self._read_file(dataset=dataset)
            return dataset
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading dataset {dataset.name}. File containing dataset contents was not found at {dataset.storage_location}.\n{e}"
            self._logger.exception(msg)
            raise DatasetIntegrityError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading dataset {dataset.name} contents from file."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
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

    # -------------------------------------------------------------------------------------------- #
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

    # -------------------------------------------------------------------------------------------- #
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

    # -------------------------------------------------------------------------------------------- #
    #                               DATASET LIST METHODS                                           #
    # -------------------------------------------------------------------------------------------- #
    def list_all(self) -> pd.DataFrame:
        """
        Lists all datasets' metadata.

        Returns:
            pd.DataFrame: A DataFrame containing metadata for all datasets.
        """
        return self._dataset_dao.read_all()

    # -------------------------------------------------------------------------------------------- #
    def list_by_phase(self, phase: PhaseDef) -> pd.DataFrame:
        """
        Lists datasets filtered by the specified phase.

        Args:
            phase (PhaseDef): The phase to filter datasets by.

        Returns:
            pd.DataFrame: A DataFrame containing metadata for the filtered datasets.
        """
        return self._dataset_dao.read_by_phase(phase=phase)

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET REMOVAL METHODS                                          #
    # -------------------------------------------------------------------------------------------- #
    def remove(self, name: str, ignore_errors: bool = False) -> None:
        """
        Removes a dataset and its associated file(s) from the repository.

        This method attempts to remove both the dataset object and its related file(s).
        If the dataset object does not exist, and `ignore_errors` is set to True, the method will still
        search for and remove any related dataset files. If `ignore_errors` is False, it will raise an exception
        if the dataset object or file cannot be found or deleted.

        Args:
            name (str): The name of the dataset to be removed.
            ignore_errors (bool): If True, suppresses any exceptions during the removal process
                and logs warnings instead of raising errors. Default is False.

        Raises:
            DatasetRemovalError: If any error occurs while removing the dataset object or file, and `ignore_errors` is False.
        """
        # Obtain the dataset object and storage information from the repository
        try:
            dataset = self._dataset_dao.read(name=name)
            # Delete the dataset file from the repository
            self._remove_dataset_file_by_filepath(
                filepath=dataset.storage_location, ignore_errors=ignore_errors
            )
        except ObjectNotFoundError:
            msg = f"Exception occurred while removing dataset {name}.\nDataset object does not exist."
            if ignore_errors:
                msg += "\nSearching for and removing any related dataset files."
                self._logger.warning(msg)
                self._remove_dataset_file_by_name(
                    name=name, ignore_errors=ignore_errors
                )
            else:
                self._logger.exception(msg)
                raise DatasetRemovalError(msg)
        except Exception as e:
            msg = f"Exception occurred while removing dataset {name}. Unable to read dataset object."
            if ignore_errors:
                msg += "Searching for and removing any related dataset files."
                self._logger.warning(msg)
                self._remove_dataset_file_by_name(
                    name=name, ignore_errors=ignore_errors
                )
            else:
                self._logger.exception(msg)
                raise DatasetRemovalError(msg, e) from e

        # Delete the dataset object from the repository.
        try:
            self._dataset_dao.delete(name=name)
        except Exception as e:
            msg = f"Exception occurred while removing dataset object {name} from the repository.\n{e}"
            if ignore_errors:
                self._logger.warning(msg)
            else:
                self._logger.exception(msg)
                raise DatasetRemovalError(msg, e)

        msg = f"Removed dataset {name} from the repository."
        self._logger.info(msg)

    # -------------------------------------------------------------------------------------------- #
    def _remove_dataset_file_by_name(
        self, name: str, ignore_errors: bool = False
    ) -> None:
        """
        Removes the dataset file associated with the dataset name.

        This method reconstructs the filepath from the dataset name and attempts to remove the dataset file.
        If the file does not exist, it will check for a non-partitioned Parquet version and attempt to remove that file.

        Args:
            name (str): The name of the dataset whose file is to be removed.
            ignore_errors (bool): If True, suppresses any exceptions during the file removal process
                and logs warnings instead of raising errors. Default is False.

        Raises:
            DatasetRemovalError: If any error occurs during the file removal process and `ignore_errors` is False.
        """
        # Reconstruct a filepath based on name
        filepath = self._reconstruct_filepath(name=name, ignore_errors=ignore_errors)

        if filepath:
            # Remove it if it exists.
            if self._cfs_dao.exists(filepath=filepath):
                self._remove_dataset_file_by_filepath(
                    filepath=filepath, ignore_errors=ignore_errors
                )
            else:
                # Search and remove a non-partitioned parquet version file.
                filepath = filepath + ".parquet"
                if self._cfs_dao.exists(filepath=filepath):
                    self._remove_dataset_file_by_filepath(
                        filepath=filepath, ignore_errors=ignore_errors
                    )

    # -------------------------------------------------------------------------------------------- #
    def _remove_dataset_file_by_filepath(
        self, filepath: str, ignore_errors: bool = False
    ) -> None:
        """
        Removes the dataset file located at the specified filepath.

        This method attempts to delete the file located at the given filepath. If an error occurs during deletion
        and `ignore_errors` is False, it will raise an exception. If `ignore_errors` is True, it will log a warning
        instead of raising an exception.

        Args:
            filepath (str): The path of the dataset file to be removed.
            ignore_errors (bool): If True, suppresses any exceptions during the file removal process
                and logs warnings instead of raising errors. Default is False.

        Raises:
            DatasetRemovalError: If an error occurs during file removal and `ignore_errors` is False.
        """
        try:
            self._cfs_dao.delete(filepath)
            msg = f"Removed dataset file at {filepath} from repository."
            self._logger.info(msg)
        except Exception as e:
            msg = f"Exception occurred while deleting {filepath}.\n{e}"
            if ignore_errors:
                self._logger.warning(msg)
            else:
                self._logger.exception(msg)
                raise DatasetRemovalError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET EXISTENCE METHOD                                         #
    # -------------------------------------------------------------------------------------------- #
    def exists(self, name: str) -> bool:
        """
        Checks if a dataset exists by its ID.

        Args:
            name (str): The name to check for existence.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        return self._dataset_dao.exists(name=name)

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET FILEPATH METHODS                                         #
    # -------------------------------------------------------------------------------------------- #

    def _format_filepath(
        self, phase: PhaseDef, stage: StageDef, partitioned: bool = True
    ) -> str:
        """
        Dynamically generates the dataset filepath based on the given phase, stage, and partitioning status.

        This method constructs a filename using the values of the provided `phase` and `stage`.
        It appends ".parquet" to the filename if the dataset is not partitioned. The full filepath is
        generated by combining the phase's directory with the filename, and it is further processed
        by the `filepath_service` to prepend home and environment folders.

        Args:
            phase (PhaseDef): The phase definition containing the directory and phase value.
            stage (StageDef): The stage definition containing the stage value.
            partitioned (bool): If True, the filename does not include a ".parquet" extension,
                indicating it is partitioned. If False, the ".parquet" extension is added.

        Returns:
            str: The fully formatted dataset filepath.
        """
        filename = f"{phase.value}_{stage.value}"
        filename = filename + ".parquet" if not partitioned else filename
        filepath = os.path.join(phase.directory, filename)
        return filepath_service.get_filepath(filepath=filepath)

    # -------------------------------------------------------------------------------------------- #

    def _reconstruct_filepath(
        self, name: str, ignore_errors: bool = False
    ) -> Optional[str]:
        """
        Reconstructs the filepath for a dataset based on its name.

        This method extracts the phase from the dataset's name, reconstructs the filepath
        by combining the phase's directory and the dataset name, and processes it through
        the `filepath_service` to prepend the home and environment folders.

        Args:
            name (str): The name of the dataset, typically in the format "{phase}_{stage}".
            ignore_errors (bool): Ignore exceptions if True.

        Returns:
            str: The fully reconstructed dataset filepath.

        Raises:
            ValueError if the name provided is not valid
        """
        phase_value = name.split("_")[0]
        try:
            phase = PhaseDef.from_value(value=phase_value)
            filepath = os.path.join(phase.directory, name)
            return filepath_service.get_filepath(filepath=filepath)
        except ValueError as e:
            msg = f"Exception while reconstructing the filepath for dataset {name}. The name doesn't include a valid phase. Unable to reconstruct the dataset filepath.\n{e}"
            if ignore_errors:
                self._logger.warning(msg)
            else:
                self._logger.exception(msg)
                raise
