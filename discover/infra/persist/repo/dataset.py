#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/persist/repo/dataset.py                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday December 23rd 2024 02:46:53 pm                                               #
# Modified   : Friday January 24th 2025 09:37:27 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from discover.asset.base.asset import AssetType
from discover.asset.base.repo import Repo
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetPassport
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader
from discover.infra.exception.object import ObjectExistsError, ObjectNotFoundError
from discover.infra.persist.repo.file.fao import FAO
from discover.infra.persist.repo.object.dao import DAO
from discover.infra.persist.repo.object.rao import RAO
from discover.infra.utils.file.fileset import FileAttr


# ------------------------------------------------------------------------------------------------ #
#                                      DATASET REPO                                                #
# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(Repo):
    """Repository for managing dataset datasets and their associated files.

    This repository extends `DatasetRepo` to provide specialized operations for managing datasets,
    including the addition, retrieval, and removal of associated files in both Pandas and Spark
    environments. It ensures that files on disk are properly handled when a dataset is added,
    retrieved, deleted, or when the repository is reset.

    Args:
        location (str): The base directory for storing files.
        dao (DAO): The data access object used for persistence of dataset metadata.
        fao (FAO): File access object for file persistence.
        rao (RAO): Registry access object for maintaining the repository registry.

    """

    __ASSET_TYPE = AssetType.DATASET

    def __init__(self, location: str, dao: DAO, fao: FAO, rao: RAO) -> None:
        super().__init__()  # base class assigns the value to self._dao
        self._location = location
        self._dao = dao
        self._fao = fao
        self._rao = rao

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def count(self) -> int:
        """Gets the number of datasets in the repository.

        Returns:
            int: The count of datasets in the repository.
        """
        return self._rao.count

    @property
    def registry(self) -> pd.DataFrame:
        """Returns the repository registry."""
        return self._rao.read_all()

    def add(self, dataset: Dataset, entity: str = None) -> Dataset:
        """Adds a Dataset dataset to the repository.

        Args:
            dataset (Dataset): The dataset object to be added to the repository.
            entity (str): The class name adding the dataset.

        Returns:
            Dataset: The dataset
        """

        self._logger.info(f"Dataset {dataset.asset_id} being published by {entity}")

        # 1.  Update the Dataset's status to `PUBLISHED`
        dataset.publish(entity=entity)

        # 2. Determine filepath.
        filepath = self._get_filepath(dataset=dataset)

        # 3. Persist the DataFrame to file.
        self._fao.create(
            filepath=filepath,
            file_format=dataset.passport.file_format,
            dataframe=dataset.dataframe,
            overwrite=False,
        )

        # 4. Now that the file(s) have been persisted, add the fileset metadata object to the dataset
        dataset = self._set_fileset(filepath=filepath, dataset=dataset)

        # 5. Create the Dataset metadata object.
        self._dao.create(asset=dataset)

        # 6. Add the Dataset to the registry
        self._rao.create(asset=dataset)

        return dataset

    def get(
        self,
        asset_id: str,
        spark: Optional[SparkSession] = None,
        dftype: Optional[DFType] = None,
        entity: Optional[str] = None,
    ) -> "Dataset":
        """
        Retrieve a Dataset by its dataset ID and load its data into memory.

        Args:
            asset_id (str): The identifier of the dataset to retrieve.
            spark (Optional[SparkSession]): The Spark session for distributed dataframes.
            dftype (Optional[DFType]): The dataframe type to return. If not provided, it returns
                the type designated in the dataset. This allows datasets saved as pandas dataframes to
                be read using another spark or another dataframe type.
            entity (str): Class name requesting the dataset.

        Returns:
            Dataset: The reconstituted dataset with its data loaded.

        Note:
            This method uses `setattr` to update the internal `_data` attribute
            of the `Dataset`'s `data` object, ensuring immutability in the public API.
        """
        # 1. Obtain the dataset metadata object
        dataset = self._dao.read(asset_id=asset_id)

        # 2. If the dftype has not been provided, we'll use the dftype native to the dataset.
        dftype = dftype or dataset.passport.dftype

        # 3. Read the DataFrame from file
        df = self._fao.read(filepath=dataset.file.path, dftype=dftype, spark=spark)

        # 4. Deserialize the dataframe
        dataset.deserialize(dataframe=df)

        # 5. Mark the dataset as accessed.
        dataset.access(entity=entity)

        # 6. Update the Dataset object metadata
        self._dao.update(asset=dataset)

        # Update the registry accordingly
        self._rao.update(asset=dataset)
        return dataset

    def get_meta(
        self,
        asset_id: str,
        entity: Optional[str] = None,
    ) -> DatasetPassport:
        """Returns the dataset's metadata

        Args:
            asset_id (str): The identifier of the dataset passport to retrieve.
            entity (str): Class name requesting the dataset.

        Returns:
            DatasetPassport: The dataset's passport
        """
        dataset_meta = self._dao.read(asset_id=asset_id)
        return dataset_meta

    def get_asset_id(self, phase: PhaseDef, stage: StageDef, name: str) -> str:
        """Returns an asset id given the parameters

        Args:
            phase (PhaseDef): The phase for which the asset is being requested
            stage (StageDef): The stage for which the asset is being requested
            name (str): The name of the asset

        Returns:
            str: The asset id associated with the given parameters

        """
        return f"{phase.value}_{stage.value}_{self.__ASSET_TYPE.value}_{name}"

    def gen_asset_id(
        self, phase: PhaseDef, stage: StageDef, name: str
    ) -> Optional[str]:
        """Generates a new asset id given the parameters.

        Similar to the get_asset_id method; except this method is designed to  generate a new
        asset id for a new Dataset. If a Dataset exists with the generated asset id, an exception
        is raised.

        Args:
            phase (PhaseDef): The phase for which the asset is being requested
            stage (StageDef): The stage for which the asset is being requested
            name (str): The name of the asset

        Returns:
            Optional[str]: The asset id associated with the given parameters if it doesn't already exist.

        Raises:
            ObjectExistsError: If an object exists with the asset id

        """
        asset_id = self.get_asset_id(phase=phase, stage=stage, name=name)
        if self._rao.exists(asset_id):
            msg = f"ObjectExistsError: A Dataset already exists with id {asset_id}."
            self._logger.error(msg)
            raise ObjectExistsError(msg)

        return asset_id

    def update(self, dataset: Dataset) -> None:
        """Updates the Dataset (metadata) and registry.

        Args:
            dataset (Dataset): The dataset object to be updated.
            entity (str): Class name requesting the dataset.

        Returns:
            None
        """
        self._dao.update(dataset=dataset)
        self._rao.update(asset=dataset)

    def exists(self, asset_id: str) -> bool:
        """Evaluates existence of the designated dataset.

        The registry is the source of truth w.r.t to repository contents. The existence
         of the dataset components are not physically checked as dataset integrity
         is handled separately.

        Args:
            asset_id (str): The identifier for the asset to check.

        Returns:
            bool: True if an asset exists with the designated asset_id, false otherwise.

        Raises:
            DataIntegrityError if the dataset metadata exists and the file doesn't (or vice-versa).
        """
        return self._rao.exists(asset_id=asset_id)

    def remove(self, asset_id: str) -> None:
        """Removes a dataset and its associated file from the repository.

        This is a fail-safe, idempotent approach  to remove behavior. By avoiding exceptions,
        non existent files and metadata are handled gracefully, prioritizing stability
        and resilience.

        Args:
            asset_id (str): The unique identifier of the dataset to remove.

        Logs:
            Info: Logs the successful removal of the dataset and its file.

        Raises:
            ValueError: If the file or directory specified by the dataset's filepath
            does not exist or cannot be identified.
        """

        dataset_meta = None
        try:
            # Get the dataset metadata containing the dataframe's filepath.
            dataset_meta = self.get_meta(asset_id=asset_id)
        except ObjectNotFoundError:
            msg = f"Attempting to remove a dataset {asset_id}, that does not exist."
            self._logger.warning(msg)
        except Exception as e:
            msg = f"An unknown exception occurred while removing dataset {asset_id} metadata from the repository.\n{e}"
            self._logger.exception(msg)

        if dataset_meta is not None:
            # Delete the files from the respository
            try:
                self._fao.delete(filepath=dataset_meta.file.path)
            except FileNotFoundError:
                msg = f"No files for dataset {asset_id} were found to remove."
                self._logger.warning(msg)
            except Exception as e:
                msg = f"An unknown exception occurred while removing dataset {asset_id} files from the repository.\n{e}"
                self._logger.exception(msg)

            # Remove the dataset metadata object from the repository
            self._dao.delete(asset_id=asset_id)

            # Remove the dataset from the registry
            self._rao.delete(asset_id=asset_id)
            self._logger.debug(
                f"Dataset {dataset_meta.asset_id}, including its file at {dataset_meta.file.path} has been removed from the repository."
            )

    def reset(self) -> None:
        """Resets the repository by removing all datasets and their associated files.

        The reset operation is irreversible and requires user confirmation.

        Logs:
            Warning: Logs a warning if the repository is successfully reset.
            Info: Logs information if the reset operation is aborted.

        Raises:
            ValueError: If any dataset's filepath does not exist or cannot be identified.
        """
        if AppConfigReader().get_environment().lower() == "test":
            asset_ids = self.get_all(keys_only=True)

            self._logger.info(f"Datasets to be deleted: {self.count}")

            for asset_id in asset_ids:
                self.remove(asset_id=asset_id)
            self._logger.warning(
                f"{self.__class__.__name__} has been reset. Current dataset count: {self.count}"
            )
        else:
            msg = "Repository reset is only supported in test environment."
            self._logger.error(msg)
            raise RuntimeError(msg)

    def _get_filepath(self, dataset: Dataset) -> str:
        """Assigns and returns the filepath  for the dataset.

        Args:
            dataset (Dataset): The dataset object.

        Returns:
            str: The path to the file containing the dataset's dataframe

        Raises:
            TypeError: If any required arguments are missing or invalid.
            Exception: If an unknown exception occurs.
        """
        try:
            # Construct the file extension and filename
            filext = dataset.passport.file_format.ext.lstrip(
                "."
            )  # Remove leading dot if present
            filename = f"{dataset.asset_id}.{filext}"

            # Use Path to construct the full file path and convert it to string
            return str(Path(self._location) / dataset.phase.value / filename)

        except AttributeError as e:
            msg = "An TypeError occurred while creating the filepath."
            msg += f"asset_id: Expected a string type, received a {type(dataset.asset_id)} object.\n"
            msg += f"phase: Expected a PhaseDef type, received a {type(dataset.phase)} object.\n"
            msg += f"file_format: Expected a FileFormat type, received a {type(dataset.passport.file_format)} object.\n{e}"
            raise TypeError(msg)
        except Exception as e:
            msg = f"Un expected exception occurred while creating filepath.\n{e}"

    def _set_fileset(self, filepath: str, dataset: Dataset) -> Dataset:
        """Obtain the fileset metadata for the filepath and update the dataset.

        Args:
            dataset (Dataset): The dataset object.

        Returns:
            Dataset: Dataset containing the FileSet object

        """
        file = FileAttr.get_fileset(
            filepath=filepath, file_format=dataset.passport.file_format
        )

        dataset.file = file
        return dataset
