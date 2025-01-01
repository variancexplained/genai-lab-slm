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
# Modified   : Tuesday December 31st 2024 08:36:33 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from typing import TYPE_CHECKING, Optional

from pyspark.sql import SparkSession

from discover.asset.base.asset import Asset
from discover.asset.dataset import DFType
from discover.infra.persist.dataframe.base import DataFrame
from discover.infra.persist.file.fao import FAO
from discover.infra.persist.object.base import DAO
from discover.infra.persist.repo.base import AssetRepo

# ------------------------------------------------------------------------------------------------ #
if TYPE_CHECKING:
    from discover.asset.dataset.dataset import Dataset


# ------------------------------------------------------------------------------------------------ #
#                                      DATASET REPO                                                #
# ------------------------------------------------------------------------------------------------ #
class DatasetRepo(AssetRepo):
    """Repository for managing dataset assets and their associated files.

    This repository extends `AssetRepo` to provide specialized operations for managing datasets,
    including the addition, retrieval, and removal of associated files in both Pandas and Spark
    environments. It ensures that files on disk are properly handled when a dataset is added,
    retrieved, deleted, or when the repository is reset.

    Args:
        dao (DAO): The data access object used for persistence of dataset metadata.
        pandas_fao (PandasFAO): Pandas file access object used for managing Pandas DataFrames.
        spark_fao (SparkFAO): Spark file access object used for managing Spark DataFrames.
    """

    def __init__(self, dao: DAO, fao: FAO) -> None:
        super().__init__(dao=dao)  # base class assigns the value to self._dao
        self._fao = fao

    @property
    def count(self) -> int:
        """Gets the number of datasets in the repository.

        Returns:
            int: The count of datasets in the repository.
        """
        return self._dao.count

    def add(self, asset: Asset) -> None:
        """Adds a Dataset asset to the repository.

        Args:
            asset (Dataset): The dataset object to be added to the repository.
        """

        # Persist the underlying data to file.
        self._fao.create(
            dftype=asset.data.dftype,
            filepath=asset.data.filepath,
            file_format=asset.data.file_format,
            dataframe=asset.data.dataframe,
            overwrite=False,
        )

        # Save the Dataset object.
        self._dao.create(asset=asset)

    def get(self, asset_id: str, spark: Optional[SparkSession] = None) -> "Dataset":
        """
        Retrieve a Dataset by its asset ID and load its data into memory.

        Args:
            asset_id (str): The identifier of the dataset to retrieve.
            spark (Optional[SparkSession]): The Spark session for distributed dataframes.

        Returns:
            Dataset: The reconstituted dataset with its data loaded.

        Note:
            This method uses `setattr` to update the internal `_data` attribute
            of the `Dataset`'s `data` object, ensuring immutability in the public API.
        """
        dataset = self._dao.read(asset_id=asset_id)

        df = self._get_data(
            filepath=dataset.data.filepath, dftype=dataset.data.dftype, spark=spark
        )
        setattr(dataset.data, "_dataframe", df)
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
        asset_meta = self.get_metadata(asset_id=asset_id)
        self._fao.delete(filepath=asset_meta.data.filepath)
        self._dao.delete(asset_id=asset_id)
        self._logger.info(
            f"Dataset {asset_meta.asset_id}, including its file at {asset_meta.data.filepath} has been removed from the repository."
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
        proceed = input(
            "Resetting the repository is irreversible. To proceed, type 'YES'."
        )
        if proceed == "YES":
            asset_ids = self.get_all(keys_only=True)

            self._logger.info(f"Assets to be deleted: {self.count}")

            for asset_id in asset_ids:
                self.remove(asset_id=asset_id)
            self._logger.warning(
                f"{self.__class__.__name__} has been reset. Current asset count: {self.count}"
            )
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
