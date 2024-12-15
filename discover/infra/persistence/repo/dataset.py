#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persistence/repo/dataset.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday October 8th 2024 07:31:47 pm                                                #
# Modified   : Sunday December 15th 2024 04:19:15 am                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repository Module"""
import logging
from typing import Callable, Optional, Union

import pandas as pd
import pyspark

from discover.assets.dataset import Dataset
from discover.assets.repo import Repo
from discover.core.data_structure import DataFrameType
from discover.core.flow import PhaseDef
from discover.infra.persistence.dal.dao.dataset import DatasetDAO
from discover.infra.persistence.dal.dao.exception import ObjectNotFoundError
from discover.infra.persistence.dal.fao.centralized import (
    CentralizedFileSystemFAO as FAOCFS,
)
from discover.infra.persistence.dal.fao.distributed import (
    DistributedFileSystemFAO as FAODFS,
)
from discover.infra.persistence.dal.fao.location import FileLocationService
from discover.infra.persistence.repo.exception import (
    DatasetCreationError,
    DatasetExistsError,
    DatasetIntegrityError,
    DatasetIOError,
    DatasetNotFoundError,
    DatasetRemovalError,
)

DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """
    A repository for managing datasets in both centralized and distributed environments.

    This class provides methods to create, retrieve, update, and manage datasets,
    ensuring consistency across metadata storage and file persistence.

    Args:
        dataset_dao (DatasetDAO): Data Access Object for managing dataset metadata.
        fao_cfs (FAOCFS): File access object for centralized file system interactions.
        fao_dfs (FAODFS): File access object for distributed file system interactions.
        location_service (FileLocationService): Service for determining file storage locations.
        partitioned (bool, optional): Whether to store datasets in partitioned format. Defaults to True.
    """

    def __init__(
        self,
        dataset_dao: DatasetDAO,
        fao_cfs: FAOCFS,
        fao_dfs: FAODFS,
        location_service: FileLocationService,
        partitioned: bool = True,
    ) -> None:
        self._dataset_dao = dataset_dao
        self._fao_cfs = fao_cfs
        self._fao_dfs = fao_dfs
        self._location_service = location_service
        self._partitioned = partitioned
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # -------------------------------------------------------------------------------------------- #
    def add(self, dataset: Dataset) -> None:
        """
        Adds a dataset to the repository.

        This method validates the dataset, determines its storage location, writes its
        content to the file system, and stores its metadata using the DAO.
        If an error occurs during persistence, it rolls back the operation to maintain consistency.

        Args:
            dataset (Dataset): The dataset to be added.

        Raises:
            DatasetIOError: If an error occurs during file I/O or metadata persistence.
            DatasetCreationError: If the dataset cannot be created due to an error.
        """
        # Ensure dataset doesn't already exist
        self._validate_add(dataset=dataset)

        # Set the storage location on the dataset object
        dataset.storage_location = self._location_service.get_filepath(
            asset_type=dataset.__class__.__name__.lower(),
            phase=dataset.phase,
            stage=dataset.stage,
            name=dataset.name,
            partition=self._partitioned,
        )

        # Save dataset contents to file.
        try:
            self._write_file(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred adding dataset {dataset.asset_id} to the repository."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

        # Save dataset object (metadata and config) to object storage
        try:
            self._dataset_dao.create(dataset=dataset)
        except Exception as e:
            msg = f"Exception occurred while saving dataset {dataset.asset_id} to object storage. Rolling back file persistence."

            # Rollback the file write to maintain consistency.
            try:
                self._remove_dataset_file_by_filepath(filepath=dataset.storage_location)
            except Exception as e:
                fileio_msg = f"Exception occurred while rolling back dataset {dataset.asset_id} persistence."
                self._logger.exception(fileio_msg)
                raise DatasetIOError(fileio_msg, e) from e

            # Raise the original exception
            raise DatasetCreationError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def get(
        self,
        asset_id: str,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> Optional[Dataset]:
        """
        Retrieves a dataset by its asset ID.

        This method fetches the dataset's metadata and content from storage, returning
        a fully populated `Dataset` object.

        Args:
            asset_id (str): The unique identifier of the dataset.
            dataframe_type (DataFrameType, optional): The type of DataFrame to return
                (e.g., Pandas, Spark). Defaults to `DataFrameType.PANDAS`.

        Returns:
            Optional[Dataset]: The retrieved dataset.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an error occurs during I/O operations.
            DatasetIntegrityError: If the dataset file is missing or inconsistent.
        """
        # Step 1: Obtain the dataset object containing metadata and config.
        try:
            dataset = self._dataset_dao.read(asset_id=asset_id)
        except ObjectNotFoundError:
            msg = f"Dataset {asset_id} does not exist."
            self._logger.exception(msg)
            raise DatasetNotFoundError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading the dataset {asset_id} object."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

        # Step 2: Get the dataset contents from file, add to dataset object and return
        try:
            dataset.content = self._read_file(
                dataset=dataset,
                dataframe_type=dataframe_type,
            )
            return dataset
        except FileNotFoundError as e:
            msg = f"Exception occurred while reading dataset {dataset.asset_id}. File containing dataset contents was not found at {dataset.storage_location}.\n{e}"
            self._logger.exception(msg)
            raise DatasetIntegrityError(msg)
        except Exception as e:
            if "[PATH_NOT_FOUND]" in str(e):
                msg = f"Exception occurred while reading dataset {dataset.asset_id}. File containing dataset contents was not found at {dataset.storage_location}.\n{e}"
                self._logger.exception(msg)
                raise DatasetIntegrityError(msg)
            else:
                msg = f"Exception occurred while reading dataset {dataset.asset_id} contents from file."
                self._logger.exception(msg)
                raise DatasetIOError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def select(
        self,
        asset_id: str,
        condition: Callable,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> pd.DataFrame:
        """
        Selects rows from a dataset based on a condition.

        This method retrieves the dataset and applies the specified condition to filter rows.

        Args:
            asset_id (str): The unique identifier of the dataset.
            condition (Callable): A function representing the condition to apply to the DataFrame.
            dataframe_type (DataFrameType, optional): The type of DataFrame to use. Defaults to `DataFrameType.PANDAS`.

        Returns:
            pd.DataFrame: The filtered DataFrame.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an error occurs during retrieval or processing.
        """
        df = self.get(asset_id=asset_id, dataframe_type=dataframe_type)
        return df[condition]

    def get_dataset_metadata(self, asset_id: str) -> Dataset:
        """
        Retrieves metadata for a dataset by its asset ID.

        Args:
            asset_id (str): The unique identifier of the dataset.

        Returns:
            Dataset: The dataset object containing metadata.

        Raises:
            DatasetNotFoundError: If the dataset does not exist.
            DatasetIOError: If an error occurs during metadata retrieval.
        """
        try:
            return self._dataset_dao.read(asset_id=asset_id)
        except ObjectNotFoundError:
            msg = f"Dataset {asset_id} does not exist."
            self._logger.exception(msg)
            raise DatasetNotFoundError(msg)
        except Exception as e:
            msg = f"Exception occurred while reading the dataset {asset_id} object."
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def update_dataset_metadata(self, dataset: Dataset) -> None:
        """
        Updates metadata for a given dataset.

        Args:
            dataset (Dataset): The dataset with updated metadata.

        Raises:
            DatasetIOError: If the metadata cannot be updated.
        """
        try:
            return self._dataset_dao.update(dataset=dataset)
        except Exception as e:
            msg = f"Metadata for dataset {dataset.asset_id} could not be updated.\n{e}"
            self._logger.exception(msg)
            raise DatasetIOError(msg, e) from e

    # -------------------------------------------------------------------------------------------- #
    def _read_file(
        self,
        dataset: Dataset,
        dataframe_type: DataFrameType = DataFrameType.PANDAS,
    ) -> DataFrame:
        """
        Reads the content of a dataset from storage.

        This method determines the appropriate read operation based on the
        specified DataFrame type (e.g., centralized or distributed).

        Args:
            dataset (Dataset): The dataset whose content is to be read.
            dataframe_type (DataFrameType, optional): The type of DataFrame to return. Defaults to `DataFrameType.PANDAS`.

        Returns:
            DataFrame: The dataset content as a DataFrame.

        Raises:
            DatasetIOError: If an error occurs during file reading.
        """
        if dataframe_type.distributed:
            return self._read_distributed_file(dataset=dataset, nlp=dataframe_type.nlp)
        else:
            return self._read_centralized_file(dataset=dataset)

    # -------------------------------------------------------------------------------------------- #
    def _read_centralized_file(self, dataset: Dataset) -> pd.DataFrame:
        """
        Reads a dataset's content from a centralized file system.

        Args:
            dataset (Dataset): The dataset to read.

        Returns:
            pd.DataFrame: The content of the dataset.
        """
        return self._fao_cfs.read(filepath=dataset.storage_location)

    # -------------------------------------------------------------------------------------------- #
    def _read_distributed_file(
        self, dataset: Dataset, nlp: bool = False
    ) -> pyspark.sql.DataFrame:
        """
        Reads a dataset's content from a distributed file system.

        Args:
            dataset (Dataset): The dataset to read.

        Returns:
            pyspark.sql.DataFrame: The content of the dataset.
        """
        return self._fao_dfs.read(filepath=dataset.storage_location, nlp=nlp)

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
    def remove(self, asset_id: str, ignore_errors: bool = False) -> None:
        """
        Removes a dataset and its associated file(s) from the repository.

        This method attempts to remove both the dataset object and its related file(s).
        If the dataset object does not exist, and `ignore_errors` is set to True, the method will still
        search for and remove any related dataset files. If `ignore_errors` is False, it will raise an exception
        if the dataset object or file cannot be found or deleted.

        Args:
            asset_id (str): The id of the dataset to be removed.
            ignore_errors (bool): If True, suppresses any exceptions during the removal process
                and logs warnings instead of raising errors. Default is False.

        Raises:
            DatasetRemovalError: If any error occurs while removing the dataset object or file, and `ignore_errors` is False.
        """
        # Obtain the dataset object and storage information from the repository
        if self._dataset_dao.exists(asset_id=asset_id):
            dataset = self._dataset_dao.read(asset_id=asset_id)
            # Delete the dataset file from the repository
            self._remove_dataset_file_by_filepath(filepath=dataset.storage_location)
            # Delete the dataset object.
            self._dataset_dao.delete(asset_id=asset_id)
            msg = f"Removed dataset {asset_id} from the repository."
            self._logger.debug(msg)
        # If ignoring errors, issue a warning and search for file remnants by name.
        elif ignore_errors:
            msg = f"Warning: Dataset {asset_id} does not exist."
            self._logger.warning(msg)
        # Otherwise throw a DatasetRemovalError
        else:
            msg = f"Exception: Dataset {asset_id} does not exist"
            self._logger.exception(msg)
            raise DatasetRemovalError(msg)

    # -------------------------------------------------------------------------------------------- #
    def _remove_dataset_file_by_filepath(self, filepath: str) -> None:
        """
        Removes the dataset file located at the specified filepath if it exists.

        Args:
            filepath (str): The path of the dataset file to be removed.
        """
        if self._fao_cfs.exists(filepath=filepath):
            self._fao_cfs.delete(filepath)
            msg = f"Removed dataset file at {filepath} from repository."
            self._logger.debug(msg)

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET EXISTENCE METHOD                                         #
    # -------------------------------------------------------------------------------------------- #
    def exists(self, asset_id: str) -> bool:
        """
        Checks if a dataset exists by its ID.

        Args:
            asset_id (str): The id to check for existence.

        Returns:
            bool: True if the dataset exists, False otherwise.
        """
        return self._dataset_dao.exists(asset_id=asset_id)

    # -------------------------------------------------------------------------------------------- #
    def _validate_add(self, dataset: Dataset) -> None:
        """Ensures dataset object and file doesn't already exist"""
        if self.exists(asset_id=dataset.asset_id):
            msg = f"Unable to add dataset {dataset.asset_id} as it already exists."
            self._logger.error(msg)
            raise DatasetExistsError(msg)

    # -------------------------------------------------------------------------------------------- #
    #                             DATASET WRITE METHODS                                            #
    # -------------------------------------------------------------------------------------------- #
    def _write_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content.

        Args:
            dataset (Dataset): The dataset to write.
        """
        if dataset.distributed:
            self._write_distributed_file(dataset=dataset)
        else:
            self._write_centralized_file(dataset=dataset)

    # -------------------------------------------------------------------------------------------- #
    def _write_centralized_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content to a centralized file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._fao_cfs._write(
            filepath=dataset.storage_location,
            data=dataset.content,
        )

    # -------------------------------------------------------------------------------------------- #
    def _write_distributed_file(self, dataset: Dataset) -> None:
        """
        Writes a dataset's content to a distributed file system.

        Args:
            dataset (Dataset): The dataset to write.
        """
        self._fao_dfs._write(
            filepath=dataset.storage_location,
            data=dataset.content,
        )
