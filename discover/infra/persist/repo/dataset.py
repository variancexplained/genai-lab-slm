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
# Modified   : Sunday December 29th 2024 04:22:40 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from typing import Optional

from pyspark.sql import SparkSession

from discover.asset.base.asset import Asset
from discover.asset.dataset import DFType, FileFormat
from discover.infra.persist.dataframe.base import DataFrame
from discover.infra.persist.file.fao import FAO
from discover.infra.persist.object.base import DAO
from discover.infra.persist.repo.base import AssetRepo


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
        super().__init__(dao=dao)
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
            created=asset.data.created,
            data=asset.data.as_df(),
            overwrite=False,
        )
        # Save the Dataset object.
        self._dao.create(asset=asset)

    def get_data(
        self,
        filepath: str,
        file_format: FileFormat,
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

        return self._fao.read(
            filepath=filepath, file_format=file_format, dftype=dftype, spark=spark
        )

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
        asset = self.get(asset_id=asset_id)
        self._fao.delete(filepath=asset.filepath)
        self._dao.delete(asset_id=asset_id)
        self._logger.info(
            f"Dataset {asset.asset_id}, including its file at {asset.filepath} has been removed from the repository."
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
            assets = self.get_all()
            self._logger.info(f"Assets to be deleted: {len(assets)}")
            for asset_id, asset in assets.items():
                try:
                    self._fao.delete(filepath=asset.data.filepath)
                except AttributeError as e:
                    msg = f"Asset {asset_id} file(s) could not be deleted.\n{e}"
                    self._logger.warning(msg)
                except Exception as e:
                    msg = f"Asset {asset_id} file(s) could not be deleted. An unknown exception occurred.\n{e}"
                    self._logger.error(msg)
                    raise Exception(msg)
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")
