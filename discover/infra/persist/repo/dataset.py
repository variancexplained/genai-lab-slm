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
# Modified   : Thursday January 23rd 2025 06:24:53 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

from discover.asset.base.asset import AssetType
from discover.asset.base.repo import Repo
from discover.asset.dataset.dataset import Dataset
from discover.asset.dataset.identity import DatasetPassport
from discover.core.dtypes import DFType
from discover.core.flow import PhaseDef, StageDef
from discover.infra.config.app import AppConfigReader
from discover.infra.exception.object import ObjectExistsError
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
        super().__init__(dao=dao)  # base class assigns the value to self._dao
        self._location = location
        self._fao = fao
        self._rao = rao

    @property
    def count(self) -> int:
        """Gets the number of datasets in the repository.

        Returns:
            int: The count of datasets in the repository.
        """
        return self._rao.count

    def add(self, dataset: Dataset) -> Dataset:
        """Adds a Dataset dataset to the repository.

        Args:
            dataset (Dataset): The dataset object to be added to the repository.

        Returns:
            Dataset: The dataset
        """

        # 1. Determine filepath.
        filepath = self._get_filepath(
            phase=dataset.phase,
            asset_id=dataset.asset_id,
            file_format=dataset.file.format,
        )

        # 2. Persist the DataFrame to file.
        self._fao.create(
            dftype=dataset.passport.dftype,
            filepath=filepath,
            file_format=dataset.passport.file_format,
            dataframe=dataset.dataframer.dataframe,
            overwrite=False,
        )

        # 3.  Update the Dataset's status to `PUBLISHED`
        dataset.publish()

        # 4. Now that the file(s) have been persisted, add the fileset metadata object to the dataset
        dataset = self._set_fileset(filepath=filepath, dataset=dataset)

        # 5. Update the Dataset object metadata
        self._dao.update(dataset=dataset)

        # 6. Add the Dataset to the registry
        self._rao.create(asset=dataset)

        return dataset

    def get(
        self,
        asset_id: str,
        spark: Optional[SparkSession] = None,
        dftype: Optional[DFType] = None,
    ) -> "Dataset":
        """
        Retrieve a Dataset by its dataset ID and load its data into memory.

        Args:
            asset_id (str): The identifier of the dataset to retrieve.
            spark (Optional[SparkSession]): The Spark session for distributed dataframes.
            dftype (Optional[DFType]): The dataframe type to return. If not provided, it returns
                the type designated in the dataset. This allows datasets saved as pandas dataframes to
                be read using another spark or another dataframe type.

        Returns:
            Dataset: The reconstituted dataset with its data loaded.

        Note:
            This method uses `setattr` to update the internal `_data` attribute
            of the `Dataset`'s `data` object, ensuring immutability in the public API.
        """
        # 1. Obtain the dataset metadata object
        dataset = self._dao.read(asset_id=asset_id)

        # 2. If the dftype has not been provided, we'll use the dftype native to the dataset.
        dftype = dftype or dataset.dftype

        # 3. Read the DataFrame from file
        df = self._fao.read(filepath=dataset.file.path, dftype=dftype, spark=spark)

        # 4. Deserialize the dataframe
        dataset.deserialize(dataframe=df)

        # 5. Mark the dataset as accessed.
        dataset.access()

        # 6. Update the Dataset object metadata
        self._dao.update(dataset=dataset)

        # Update the registry accordingly
        self._rao.update(asset=dataset)
        return dataset

    def get_meta(self, asset_id: str) -> DatasetPassport:
        """Returns the dataset's metadata

        Args:
            asset_id (str): The identifier of the dataset passport to retrieve.

        Returns:
            DatasetPassport: The dataset's passport
        """
        return self._dao.read(asset_id=asset_id)

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
        if self._dao.exists(asset_id):
            msg = f"ObjectExistsError: A Dataset already exists with id {asset_id}."
            self._logger.error(msg)
            raise ObjectExistsError(msg)

    def update(self, dataset: Dataset) -> None:
        """Updates the Dataset (metadata) and registry.

        Args:
            dataset (Dataset): The dataset object to be updated.

        Returns:
            None
        """
        self._dao.update(dataset=dataset)
        self._rao.update(asset=dataset)

    def exists(self, asset_id: str) -> bool:
        """Evaluates existence of the asset with the designated asset_id

        Checks the dataset metadata object and files for existence and returns True iff both dataset metadata
        and the file(s) exist. If both checks don't agree, a data integrity exception is raised.

        Args:
            asset_id (str): The identifier for the asset to check.

        Returns:
            bool: True if an asset exists with the designated asset_id, false otherwise.

        Raises:
            DataIntegrityError if the dataset metadata exists and the file doesn't (or vice-versa).
        """
        dataset = self._dao.read(asset_id=asset_id)
        if dataset is not None:
            if self._fao.exists(filepath=dataset.file.path):
                return True
            else:
                msg = f"DataIntegrityError. Dataset {asset_id} fileset does not exist."
                self._logger.error(msg)
                raise FileNotFoundError(msg)
        else:
            return False

    def remove(self, asset_id: str) -> None:
        """Removes a dataset and its associated file from the repository.

        Args:
            asset_id (str): The unique identifier of the dataset to remove.

        Logs:
            Info: Logs the successful removal of the dataset and its file.

        Raises:
            ValueError: If the file or directory specified by the dataset's filepath
            does not exist or cannot be identified.
        """
        # Get the datasets filepath from its file object.
        dataset_meta = self.get_metadata(asset_id=asset_id)
        # Delete the files from the respository
        self._fao.delete(filepath=dataset_meta.passport.filepath)
        # Update the registry before deleting the object
        dataset_meta.remove()
        self._rao.update(asset=dataset_meta)
        # Remove the Dataset object and metadata from the repository
        self._dao.delete(asset_id=asset_id)

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
