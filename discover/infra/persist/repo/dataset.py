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
# Modified   : Wednesday January 22nd 2025 03:13:01 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from typing import Optional

import pandas as pd
from pyspark.sql import SparkSession

from discover.asset.base.repo import Repo
from discover.asset.dataset.dataset import Dataset
from discover.core.dtypes import DFType
from discover.infra.config.app import AppConfigReader
from discover.infra.persist.repo.file.base import DataFrame
from discover.infra.persist.repo.file.fao import FAO
from discover.infra.persist.repo.object.dao import DAO
from discover.infra.persist.repo.object.rao import RAO


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
        dao (DAO): The data access object used for persistence of dataset metadata.
        fao (FAO): File access object for file persistence.
        rao (RAO): Registry access object for maintaining the repository registry.
    """

    def __init__(self, dao: DAO, fao: FAO, rao: RAO) -> None:
        super().__init__(dao=dao)  # base class assigns the value to self._dao
        self._fao = fao
        self._rao = rao

    @property
    def count(self) -> int:
        """Gets the number of datasets in the repository.

        Returns:
            int: The count of datasets in the repository.
        """
        return self._rao.count

    def add(self, dataset: Dataset, dftype: DFType) -> None:
        """Adds a Dataset dataset to the repository.

        Args:
            dataset (Dataset): The dataset object to be added to the repository.
        """

        # Update the Dataset status to `PUBLISHED`
        dataset.publish()

        # Persist the underlying data to file.
        if isinstance(
            dataset.dataframe, (pd.DataFrame, pd.core.frame.DataFrame, DataFrame)
        ):
            self._fao.create(
                dftype=dftype,
                filepath=dataset.file.path,
                file_format=dataset.file.format,
                dataframe=dataset.dataframer.dataframe,
                overwrite=False,
            )

        # Save the Dataset object.
        self._dao.create(dataset=dataset)

        # Register the Dataset
        self._rao.create(asset=dataset)

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
        # Obtain the dataset object and set the dftype
        dataset = self._dao.read(asset_id=asset_id)
        dftype = dftype or dataset.dftype

        # Read the DataFrame from file
        df = self._get_data(filepath=dataset.file.path, dftype=dftype, spark=spark)

        # Deserialize the dataframe
        dataset.deserialize(dataframe=df)

        # Mark the dataset accessed.
        dataset.access()

        # Update the registry accordingly
        self._rao.create(asset=dataset)
        return dataset

    def get_metadata(self, asset_id: str) -> "Dataset":
        """Returns the metadata for a dataset object

        Args:
            asset_id (str): The identifier of the dataset to retrieve.

        Returns:
            Dataset: The dataset without the dataframe.
        """
        return self._dao.read(asset_id=asset_id)

    def _get_data(
        self,
        filepath: str,
        dftype: DFType,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """Retrieves a file from the repository using the specified data structure and format.

        Args:
            filepath (str): Path to the file to retrieve.
            file_format (FileFormat): The format of the file (default is Parquet).
            dftype (DFType): The data structure (default is Pandas).
            spark (Optional[SparkSession]): Optional spark session for returning spark DataFrames.

        Returns:
            DataFrame: The data read from the file.

        Raises:
            ValueError: If an unsupported data structure is specified.
        """

        return self._fao.read(filepath=filepath, dftype=dftype, spark=spark)

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
        self._rao.create(asset=dataset_meta)
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
