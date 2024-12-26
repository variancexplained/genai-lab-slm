#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset.py                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Thursday December 26th 2024 06:36:49 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Module"""
from __future__ import annotations

from typing import Optional, Union

import pandas as pd
from dependency_injector.wiring import Provide, inject
from pydantic import validate_call
from pyspark.sql import DataFrame

from discover.asset.base import Asset
from discover.container import DiscoverContainer
from discover.core.asset import AssetType
from discover.core.data_structure import DataFrameStructureEnum
from discover.core.file import FileFormat
from discover.core.flow import (
    DataEnrichmentStageEnum,
    DataPrepStageEnum,
    ModelStageEnum,
    PhaseEnum,
)
from discover.infra.persist.repo.dataset import DatasetRepo
from discover.infra.service.spark.pool import SparkSessionPool
from discover.infra.utils.file.copy import Copy
from discover.infra.utils.file.stats import FileStats
from discover.infra.workspace.service import WorkspaceService


# ------------------------------------------------------------------------------------------------ #
#                                        DATASET                                                   #
# ------------------------------------------------------------------------------------------------ #
class Dataset(Asset):
    """Represents a dataset asset with metadata, file handling, and format conversion capabilities.

    This class extends the `Asset` class to provide specialized behavior for datasets, including
    lazy loading of data, format conversions, and integration with a dataset repository for persistence.

    Args:
        phase (PhaseEnum): The phase of the dataset lifecycle (e.g., Data Preparation, Enrichment).
        stage (Union[DataPrepStageEnum, DataEnrichmentStageEnum, ModelStageEnum]): The stage
            within the phase where the dataset belongs.
        name (str): The name of the dataset.
        repo (DatasetRepo): Repository for managing the dataset's persistence and file operations.
        description (Optional[str]): A human-readable description of the dataset.
        data (Optional[Union[pd.DataFrame, DataFrame]]): In-memory representation of the dataset,
            if available.
        dataframe_structure (Optional[DataFrameStructureEnum]): The structure of the dataset
            (e.g., Pandas, Spark).
        file_format (FileFormat): The file format used for storage (default: PARQUET).
        source_dataset_asset_id (Optional[str]): Asset ID of the source dataset, if applicable.
        parent_dataset_asset_id (Optional[str]): Asset ID of the parent dataset, if applicable.
        **kwargs: Additional arguments passed to the `Asset` superclass.
    """

    __ASSET_TYPE = AssetType.DATASET

    def __init__(
        self,
        phase: PhaseEnum,
        stage: Union[DataPrepStageEnum, DataEnrichmentStageEnum, ModelStageEnum],
        name: str,
        repo: DatasetRepo,
        description: Optional[str] = None,
        data: Optional[Union[pd.DataFrame, DataFrame]] = None,
        dataframe_structure: Optional[DataFrameStructureEnum] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        source_dataset_asset_id: Optional[str] = None,
        parent_dataset_asset_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            asset_type=self.__ASSET_TYPE,
            name=name,
            phase=phase,
            stage=stage,
            description=description,
        )

        self._data = data
        self._dataframe_structure = dataframe_structure
        self._file_format = file_format
        self._repo = repo

        self._source_dataset_asset_id = source_dataset_asset_id
        self._parent_dataset_asset_id = parent_dataset_asset_id

        # Set after instantiation by the DatasetFactory
        self._asset_id = None
        self._filepath = None

        self._size = None

        self._is_composite = False

    # --------------------------------------------------------------------------------------------- #
    #                                  DATASET PROPERTIES                                           #
    # --------------------------------------------------------------------------------------------- #
    @property
    def description(self) -> str:
        """Generates or retrieves the dataset's description."""
        self._description = self._description or self._format_description()
        return self._description

    @property
    def file_format(self) -> FileFormat:
        """The file format of the dataset."""
        return self._file_format

    @property
    def dataframe_structure(self) -> DataFrameStructureEnum:
        """The structure of the dataset (e.g., Pandas, Spark)."""
        return self._dataframe_structure

    @property
    def source_dataset_asset_id(self) -> str:
        """The asset ID of the source dataset, if available."""
        return self._source_dataset_asset_id

    @property
    def parent_dataset_asset_id(self) -> str:
        """The asset ID of the parent dataset, if available."""
        return self._parent_dataset_asset_id

    @property
    def filepath(self) -> str:
        """The file path where the dataset is stored."""
        return self._filepath

    @property
    def size(self) -> str:
        """The size of the dataset file."""
        if not self._size:
            self._size = FileStats.get_size(path=self._filepath)
        return self._size

    @property
    def accessed(self) -> str:
        """The last accessed timestamp of the dataset file."""
        return FileStats.file_last_accessed(filepath=self._filepath)

    # --------------------------------------------------------------------------------------------- #
    #                                  EXTRACT DATAFRAME                                            #
    # --------------------------------------------------------------------------------------------- #
    def as_df(self) -> Union[pd.DataFrame, DataFrame]:
        """Returns the dataset in its canonical format.

        If the data is not already loaded, it will be retrieved from the repository.

        Returns:
            Union[pd.DataFrame, DataFrame]: The dataset in its canonical format.
        """
        if self._data is None:
            self._data = self._repo.get_file(
                filepath=self._filepath,
                file_format=self._file_format,
                dataframe_structure=self._dataframe_structure,
            )
        return self._data

    def to_pandas(self) -> pd.DataFrame:
        """Returns the dataset as a Pandas DataFrame.

        Returns:
            pd.DataFrame: The dataset in Pandas format.
        """
        if self._dataframe_structure == DataFrameStructureEnum.PANDAS:
            return self.as_df()
        else:
            return self._repo.get_file(
                filepath=self._filepath,
                file_format=self._file_format,
                dataframe_structure=DataFrameStructureEnum.PANDAS,
            )

    @inject
    def to_spark(
        self,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
    ) -> DataFrame:
        """Returns the dataset as a Spark DataFrame.

        Returns:
            DataFrame: The dataset in Spark format.
        """
        if self._dataframe_structure == DataFrameStructureEnum.SPARK:
            return self.as_df()
        else:
            return self._repo.get_file(
                filepath=self._filepath,
                file_format=self._file_format,
                dataframe_structure=DataFrameStructureEnum.SPARK,
                spark=spark_session_pool.spark,
            )

    @inject
    def to_sparknlp(
        self,
        spark_session_pool: SparkSessionPool = Provide[
            DiscoverContainer.spark.session_pool
        ],
    ) -> DataFrame:
        """Returns the dataset as a SparkNLP DataFrame.

        Returns:
            DataFrame: The dataset in SparkNLP format.
        """
        if self._dataframe_structure == DataFrameStructureEnum.SPARKNLP:
            return self.as_df()
        else:
            return self._repo.get_file(
                filepath=self._filepath,
                file_format=self._file_format,
                dataframe_structure=DataFrameStructureEnum.SPARKNLP,
                spark=spark_session_pool.sparknlp,
            )

    # --------------------------------------------------------------------------------------------- #
    #                                      SERIALIZATION                                            #
    # --------------------------------------------------------------------------------------------- #
    def __getstate__(self) -> dict:
        """Prepares the object's state for serialization.

        This method converts the object's attributes into a dictionary
        that can be serialized, ensuring compatibility with serialization
        libraries and allowing the asset's state to be stored or transmitted.

        Returns:
            dict: A dictionary representation of the object's state.
        """
        state = self.__dict__.copy()
        state["_data"] = None  # Exclude data from serialization
        return state

    def __setstate__(self, state) -> None:
        """Restores the object's state during deserialization.

        Args:
            state (dict): The state dictionary to restore.
        """
        self.__dict__.update(state)

    # --------------------------------------------------------------------------------------------- #
    def _format_description(self) -> str:
        """Generates a formatted description of the dataset.

        Returns:
            str: A formatted description string.
        """
        description = ""
        description += f"Dataset {self.name} created "
        if self._source_dataset_asset_id:
            description += f"from {self._source_dataset_asset_id} "
        description += f"in the {self._phase.description} - {self._stage.description} "
        description += f"on {self._created.strftime('%Y-%m-%d')} at {self._created.strftime('H:%M:%S')}"
        return description


# ------------------------------------------------------------------------------------------------ #
#                                     DATASET FACTORY                                              #
# ------------------------------------------------------------------------------------------------ #
class DatasetFactory:
    """Factory for creating `Dataset` instances from various sources.

    This class provides methods to create datasets from files (e.g., Parquet, CSV)
    or in-memory data structures (e.g., Pandas DataFrames, Spark DataFrames). It integrates
    with a workspace service to handle metadata registration, file path resolution, and persistence.

    Args:
        config (dict): Configuration dictionary, including the file access layer (FAL) settings.
        workspace_service (WorkspaceService): Service for managing workspace operations,
            including dataset repositories and asset metadata.
    """

    @inject
    def __init__(
        self,
        config: dict = Provide[DiscoverContainer.config],
        workspace_service: WorkspaceService = Provide[
            DiscoverContainer.workspace.service
        ],
    ) -> None:
        self._workspace_service = workspace_service

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_parquet_file(
        self,
        filepath: str,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: Optional[DataFrameStructureEnum] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> Dataset:
        """Creates a `Dataset` instance from a Parquet file.

        Args:
            filepath (str): Path to the Parquet file.
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            description (Optional[str]): Description of the dataset.
            dataframe_structure (Optional[DataFrameStructureEnum]): The structure of the dataset (e.g., Pandas, Spark).
            file_format (FileFormat): File format (default: PARQUET).
            **kwargs: Additional keyword arguments.

        Returns:
            Dataset: The created dataset instance.
        """
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            description=description,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
            **kwargs,
        )

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_csv_file(
        self,
        filepath: str,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: Optional[DataFrameStructureEnum] = None,
        file_format: FileFormat = FileFormat.CSV,
        **kwargs,
    ) -> Dataset:
        """Creates a `Dataset` instance from a CSV file.

        Args:
            filepath (str): Path to the CSV file.
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            description (Optional[str]): Description of the dataset.
            dataframe_structure (Optional[DataFrameStructureEnum]): The structure of the dataset (e.g., Pandas, Spark).
            file_format (FileFormat): File format (default: CSV).
            **kwargs: Additional keyword arguments.

        Returns:
            Dataset: The created dataset instance.
        """
        return self._from_file(
            filepath=filepath,
            phase=phase,
            stage=stage,
            name=name,
            description=description,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
            **kwargs,
        )

    def _from_file(
        self,
        filepath: str,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        description: Optional[str] = None,
        dataframe_structure: Optional[DataFrameStructureEnum] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        **kwargs,
    ) -> Dataset:
        """Internal method for creating a `Dataset` instance from a file.

        Args:
            filepath (str): Path to the file.
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            description (Optional[str]): Description of the dataset.
            dataframe_structure (Optional[DataFrameStructureEnum]): The structure of the dataset (e.g., Pandas, Spark).
            file_format (FileFormat): File format (default: PARQUET).
            **kwargs: Additional keyword arguments.

        Returns:
            Dataset: The created dataset instance.
        """
        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            repo=self._workspace_service.dataset_repo,
            description=description,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
            **kwargs,
        )

        dataset = self._workspace_service.set_asset_id(asset=dataset)
        dataset = self._workspace_service.set_filepath(asset=dataset)

        copy = Copy()
        copy(source=filepath, target=dataset.filepath, overwrite=False)

        self._register_dataset(dataset=dataset)
        return dataset

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_pandas_dataframe(
        self,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        data: pd.DataFrame,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        source_dataset_asset_id: Optional[str] = None,
        parent_dataset_asset_id: Optional[str] = None,
    ) -> Dataset:
        """Creates a `Dataset` instance from a Pandas DataFrame.

        Args:
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            data (pd.DataFrame): The in-memory Pandas DataFrame.
            description (Optional[str]): Description of the dataset.
            file_format (FileFormat): File format (default: PARQUET).
            source_dataset_asset_id (Optional[str]): Asset ID of the source dataset.
            parent_dataset_asset_id (Optional[str]): Asset ID of the parent dataset.

        Returns:
            Dataset: The created dataset instance.
        """
        return self.from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=DataFrameStructureEnum.PANDAS,
            file_format=file_format,
            source_dataset_asset_id=source_dataset_asset_id,
            parent_dataset_asset_id=parent_dataset_asset_id,
        )

    @validate_call(config=dict(arbitrary_types_allowed=True))
    def from_spark_dataframe(
        self,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        data: DataFrame,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        source_dataset_asset_id: Optional[str] = None,
        parent_dataset_asset_id: Optional[str] = None,
    ) -> Dataset:
        """Creates a `Dataset` instance from a Spark DataFrame.

        Args:
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            data (DataFrame): The in-memory Spark DataFrame.
            description (Optional[str]): Description of the dataset.
            file_format (FileFormat): File format (default: PARQUET).
            source_dataset_asset_id (Optional[str]): Asset ID of the source dataset.
            parent_dataset_asset_id (Optional[str]): Asset ID of the parent dataset.

        Returns:
            Dataset: The created dataset instance.
        """
        return self.from_df(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            description=description,
            dataframe_structure=DataFrameStructureEnum.SPARK,
            file_format=file_format,
            source_dataset_asset_id=source_dataset_asset_id,
            parent_dataset_asset_id=parent_dataset_asset_id,
        )

    def from_df(
        self,
        phase: PhaseEnum,
        stage: DataPrepStageEnum,
        name: str,
        data: Union[pd.DataFrame, DataFrame],
        dataframe_structure: DataFrameStructureEnum,
        description: Optional[str] = None,
        file_format: FileFormat = FileFormat.PARQUET,
        source_dataset_asset_id: Optional[str] = None,
        parent_dataset_asset_id: Optional[str] = None,
        **kwargs,
    ) -> Dataset:
        """Creates a `Dataset` instance from an in-memory DataFrame.

        Args:
            phase (PhaseEnum): The phase of the dataset lifecycle.
            stage (DataPrepStageEnum): The stage within the phase where the dataset belongs.
            name (str): The name of the dataset.
            data (Union[pd.DataFrame, DataFrame]): The in-memory DataFrame.
            dataframe_structure (DataFrameStructureEnum): The structure of the dataset (e.g., Pandas, Spark).
            description (Optional[str]): Description of the dataset.
            file_format (FileFormat): File format (default: PARQUET).
            source_dataset_asset_id (Optional[str]): Asset ID of the source dataset.
            parent_dataset_asset_id (Optional[str]): Asset ID of the parent dataset.
            **kwargs: Additional keyword arguments.

        Returns:
            Dataset: The created dataset instance.
        """
        dataset = Dataset(
            phase=phase,
            stage=stage,
            name=name,
            data=data,
            repo=self._workspace_service.dataset_repo,
            description=description,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
            source_dataset_asset_id=source_dataset_asset_id,
            parent_dataset_asset_id=parent_dataset_asset_id,
        )

        dataset = self._workspace_service.set_asset_id(asset=dataset)
        dataset = self._workspace_service.set_filepath(asset=dataset)

        self._workspace_service.dataset_repo.add_file(
            filepath=dataset.filepath,
            data=data,
            dataframe_structure=dataframe_structure,
            file_format=file_format,
        )

        self._register_dataset(dataset=dataset)
        return dataset

    def _register_dataset(self, dataset: Dataset) -> None:
        """Registers a `Dataset` instance in the dataset repository.

        Args:
            dataset (Dataset): The dataset instance to register.
        """
        self._workspace_service.dataset_repo.add(asset=dataset)
