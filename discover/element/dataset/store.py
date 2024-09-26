#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/store.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:16 am                                              #
# Modified   : Wednesday September 25th 2024 11:10:31 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional

from discover.core.data_structure import DataStructure
from discover.core.flow import PhaseDef, StageDef
from discover.element.base.store import StorageConfig


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetStorageConfig(StorageConfig):
    """
    Base class for dataset storage configuration, handling common properties for different dataset formats and data_structures.

    This class inherits from `StorageConfig` and defines additional configurations specific to datasets, such as the data data_structure (e.g., Pandas or Spark) and whether the dataset is partitioned.

    Attributes:
    -----------
    data_structure : DataStructure
        The data data_structure of the dataset (e.g., Pandas or Spark). Default is Pandas.
    partitioned : bool
        Indicates whether the dataset is partitioned. Default is True.
    filepath : Optional[str]
        The generated file path where the dataset will be stored. Default is None until assigned.
    row_group_size : Optional[int]
        Optional size for row grouping in the dataset.
    read_kwargs : Dict[str, Any]
        A dictionary of additional arguments for reading the dataset. Defaults to an empty dictionary.
    write_kwargs : Dict[str, Any]
        A dictionary of additional arguments for writing the dataset. Defaults to an empty dictionary.

    Methods:
    --------
    format_filepath(id: int, phase: PhaseDef, stage: StageDef, name: str) -> str:
        Generates a file path for the dataset based on the phase, stage, dataset name, and ID.

    create(id: int, phase: PhaseDef, stage: StageDef, name: str, partitioned: bool = True) -> DatasetStorageConfig:
        Creates a new `DatasetStorageConfig` instance with a generated file path and partitioning settings.
    """

    data_structure: DataStructure = DataStructure.PANDAS  # Default data_structure

    # Common storage configuration variables
    partitioned: bool = True  # Indicates if the dataset is partitioned
    partition_cols: list = field(default_factory=list)
    filepath: Optional[str] = None  # The generated file path for the dataset

    read_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional read arguments
    write_kwargs: Dict[str, Any] = field(
        default_factory=dict
    )  # Additional write arguments

    # Pandas specific configuration variables
    engine: str = "pyarrow"
    compression: str = "snappy"
    index: bool = True
    existing_data_behavior: str = "delete_matching"
    row_group_size: Optional[int] = None

    # Spark specific configuration variables
    spark_session_name: str = None
    nlp: bool = False
    mode: str = None
    parquet_block_size: int = None

    @classmethod
    def format_filepath(
        cls, id: int, phase: PhaseDef, stage: StageDef, name: str
    ) -> str:
        """
        Generates a file path for the dataset using the phase, stage, name, and current date.

        Parameters:
        -----------
        id : int
            The ID of the dataset, used to create unique file paths.
        phase : PhaseDef
            The phase in which the dataset is being used (e.g., development, production).
        stage : StageDef
            The stage or step within the phase (e.g., extract, transform, load).
        name : str
            The name of the dataset.

        Returns:
        --------
        str
            A full file path with the phase, stage, dataset name, and timestamp.
        """
        filename = f"{phase.value}_{stage.value}_{name}_dataset_{datetime.now().strftime('%Y%m%d')}-{str(id).zfill(4)}"
        filepath = os.path.join(phase.directory, stage.directory, filename)
        return cls.filepath_service.get_filepath(filepath)

    @classmethod
    def create(
        cls,
        id: int,
        phase: PhaseDef,
        stage: StageDef,
        name: str,
        data_structure: DataStructure,
        read_kwargs: Dict[str, Any],
        write_kwargs: Dict[str, Any],
        partitioned: bool = True,
        partition_cols: Optional[list] = None,
        engine: Optional[str] = None,
        compression: Optional[str] = None,
        index: Optional[bool] = None,
        existing_data_behavior: Optional[str] = None,
        row_group_size: Optional[int] = None,
        spark_session_name: Optional[str] = None,
        nlp: bool = False,
        mode: Optional[str] = None,
        parquet_block_size: Optional[int] = None,
    ) -> DatasetStorageConfig:
        """ """
        filepath = cls.format_filepath(id=id, phase=phase, stage=stage, name=name)

        # Append .parquet extension if not partitioned
        filepath = filepath + ".parquet" if not partitioned else filepath

        # Return a new DatasetStorageConfig instance with the generated filepath and partitioning settings
        return cls(
            data_structure=data_structure,
            partitioned=partitioned,
            partition_cols=partition_cols,
            filepath=filepath,
            read_kwargs=read_kwargs,
            write_kwargs=write_kwargs,
            engine=engine,
            compression=compression,
            index=index,
            existing_data_behavior=existing_data_behavior,
            row_group_size=row_group_size,
            spark_session_name=spark_session_name,
            nlp=nlp,
            mode=mode,
            parquet_block_size=parquet_block_size,
        )
