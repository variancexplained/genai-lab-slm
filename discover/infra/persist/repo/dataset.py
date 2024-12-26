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
# Modified   : Thursday December 26th 2024 06:38:26 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Repo Module"""

from typing import Optional

from pyspark.sql import SparkSession

from discover.asset.base import Asset
from discover.core.data_structure import DataFrameStructureEnum
from discover.core.file import FileFormat
from discover.infra.persist.file.base import DataFrame
from discover.infra.persist.file.pandas import PandasFAO
from discover.infra.persist.file.spark import SparkFAO
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

    def __init__(
        self,
        dao: DAO,
        pandas_fao: PandasFAO,
        spark_fao: SparkFAO,
    ) -> None:
        super().__init__(dao=dao)
        self._pandas_fao = pandas_fao
        self._spark_fao = spark_fao

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
        self._dao.create(asset=asset)

    def add_file(
        self,
        filepath: str,
        data: DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
        dataframe_structure: DataFrameStructureEnum = DataFrameStructureEnum.PANDAS,
    ) -> None:
        """Adds a file to the repository using the specified data structure and format.

        Args:
            filepath (str): Path where the file should be created.
            data (DataFrame): The data to be written to the file.
            file_format (FileFormat): The format of the file (default is Parquet).
            dataframe_structure (DataFrameStructureEnum): The data structure (default is Pandas).
            spark (SparkSession): Spark session. Optional. Default = None,
        """
        if dataframe_structure == DataFrameStructureEnum.PANDAS:
            self._add_file_pandas(filepath=filepath, data=data, file_format=file_format)
        elif dataframe_structure in (
            DataFrameStructureEnum.SPARK,
            DataFrameStructureEnum.SPARKNLP,
        ):
            self._add_file_spark(filepath=filepath, data=data, file_format=file_format)

    def get_file(
        self,
        filepath: str,
        file_format: FileFormat = FileFormat.PARQUET,
        dataframe_structure: DataFrameStructureEnum = DataFrameStructureEnum.PANDAS,
        spark: Optional[SparkSession] = None,
    ) -> DataFrame:
        """Retrieves a file from the repository using the specified data structure and format.

        Args:
            filepath (str): Path to the file to retrieve.
            file_format (FileFormat): The format of the file (default is Parquet).
            dataframe_structure (DataFrameStructureEnum): The data structure (default is Pandas).

        Returns:
            DataFrame: The data read from the file.

        Raises:
            ValueError: If an unsupported data structure is specified.
        """
        if dataframe_structure == DataFrameStructureEnum.PANDAS:
            return self._get_file_pandas(filepath=filepath, file_format=file_format)
        elif dataframe_structure == DataFrameStructureEnum.SPARK:
            return self._get_file_spark(
                filepath=filepath,
                spark=spark,
                file_format=file_format,
            )
        elif dataframe_structure == DataFrameStructureEnum.SPARKNLP:
            return self._get_file_spark(
                filepath=filepath,
                spark=spark,
                file_format=file_format,
            )
        else:
            msg = f"Invalid dataframe_structure: {dataframe_structure}. Currently supporting Pandas, Spark, and SparkNLP"
            self._logger.error(msg)
            raise ValueError(msg)

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
        self._pandas_fao.delete(filepath=asset.filepath)
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
            for asset_id, asset in assets.items():
                self._pandas_fao.delete(filepath=asset.filepath)
            self._dao.reset(verified=True)
            self._logger.warning(f"{self.__class__.__name__} has been reset.")
        else:
            self._logger.info(f"{self.__class__.__name__} reset has been aborted.")

    def _add_file_pandas(
        self,
        filepath: str,
        data: DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> None:
        """Adds a Pandas DataFrame to the repository.

        Args:
            filepath (str): Path where the file should be created.
            data (DataFrame): The Pandas DataFrame to be written.
            file_format (FileFormat): The format of the file (default is Parquet).
        """
        self._pandas_fao.create(filepath=filepath, data=data, file_format=file_format)

    def _add_file_spark(
        self,
        filepath: str,
        data: DataFrame,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> None:
        """Adds a Spark DataFrame to the repository.

        Args:
            filepath (str): Path where the file should be created.
            data (DataFrame): The Spark DataFrame to be written.
            file_format (FileFormat): The format of the file (default is Parquet).
        """
        self._spark_fao.create(filepath=filepath, data=data, file_format=file_format)

    def _get_file_pandas(
        self, filepath: str, file_format: FileFormat = FileFormat.PARQUET
    ) -> DataFrame:
        """Retrieves a Pandas DataFrame from the repository.

        Args:
            filepath (str): Path to the file to retrieve.
            file_format (FileFormat): The format of the file (default is Parquet).

        Returns:
            DataFrame: The Pandas DataFrame read from the file.
        """
        return self._pandas_fao.read(filepath=filepath, file_format=file_format)

    def _get_file_spark(
        self,
        filepath: str,
        spark: SparkSession,
        file_format: FileFormat = FileFormat.PARQUET,
    ) -> DataFrame:
        """Retrieves a Spark DataFrame from the repository.

        Args:
            filepath (str): Path to the file to retrieve.
            spark (SparkSession): The Spark session to use for reading the file.
            file_format (FileFormat): The format of the file (default is Parquet).

        Returns:
            DataFrame: The Spark DataFrame read from the file.
        """
        return self._spark_fao.read(
            filepath=filepath, spark=spark, file_format=file_format
        )
