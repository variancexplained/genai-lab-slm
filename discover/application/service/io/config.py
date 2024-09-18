#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/io/config.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Wednesday September 18th 2024 03:24:34 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from discover.application.service.io.base import Reader, Writer
from discover.core.data import DataClass
from discover.domain.value_objects.data_structure import DataStructure
from discover.domain.value_objects.file import FileFormat
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataConfig(DataClass):
    """
    DataConfig represents the configuration for managing datasets within different phases and stages of
    a data pipeline. It distinguishes between the service's current stage and the input/output (I/O) stage,
    allowing for flexibility in how data is read or written.

    Attributes:
        phase (Phase): The lifecycle phase in which the service is operating. Common values include:
            - DATAPREP: Data preparation phase.
            - ANALYSIS: Data analysis phase.
            - GENAI: Generative AI phase.

        stage (Stage): The current stage of the service, such as DQA (Data Quality Assessment), CLEAN (data cleaning),
            or FEATURE (feature engineering). This represents the task that the service is currently performing.

        io_stage (Stage): The stage from which data is read or to which data is written. This allows for
            the specification of data handling based on its current state, such as reading from RAW data or writing to
            a CLEANED stage.

        name (str): The name of the dataset being processed. This identifies the specific dataset (e.g., "reviews").

        data_structure (DataStructure): The structure in which the dataset is represented. The default is
            DataStructure.PANDAS, meaning the data will be processed as a pandas DataFrame, but other structures like
            SQL tables or Spark DataFrames could also be used.

        format (FileFormat): The file format of the dataset. The default is FileFormat.PARQUET_PARTITIONED, indicating
            that the data will be stored in a partitioned Parquet format. Other formats, such as CSV or JSON, can be
            specified depending on the use case.
    """

    phase: Phase  # Lifecycle phase, i.e. DATAPREP, ANALYSIS, or GENAI
    stage: Stage  # The stage of the service, i.e. DQA, CLEAN, FEATURE.
    io_stage: Stage  # The stage from which, or to which the data are read or written.
    name: str  # The name of the dataset
    datatypes: Dict[str, Any] = field(default_factory=dict)
    data_structure: DataStructure = DataStructure.PANDAS  # The dataset data structure.
    format: FileFormat = FileFormat.PARQUET_PARTITIONED  # The dataset file format.


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ReaderConfig(DataConfig):
    """
    ReaderConfig extends the DataConfig class to provide configuration specifically for file readers.
    It includes all attributes required to define how data should be read within different phases and stages
    of a data pipeline.

    This configuration is typically used to set up the input parameters for reading datasets, including the
    data structure, file format, and the specific stage from which the data will be read.

    Inherited Attributes:
        phase (Phase): The lifecycle phase in which the reader is operating (e.g., DATAPREP, ANALYSIS, GENAI).
        stage (Stage): The service stage (e.g., DQA, CLEAN, FEATURE) during which the data is being processed.
        io_stage (Stage): The stage from which the data is read (e.g., RAW, INGEST).
        name (str): The name of the dataset being read (e.g., "reviews").
        data_structure (DataStructure): The structure of the dataset, such as DataStructure.PANDAS for pandas DataFrames.
        format (FileFormat): The file format of the dataset, such as FileFormat.PARQUET_PARTITIONED for partitioned Parquet files.
    """


# ------------------------------------------------------------------------------------------------ #
@dataclass
class WriterConfig(DataConfig):
    """
    WriterConfig extends the DataConfig class to provide configuration specifically for file writers.
    It includes attributes that define how data should be written, including file format, partitioning,
    and behavior when existing data is encountered.

    Attributes:
        partition_cols (List[str]): A list of columns by which the data should be partitioned when writing.
            This is particularly relevant when the file format is set to FileFormat.PARQUET_PARTITIONED.
            The default is an empty list, but will be set to ["category"] if the format is Parquet partitioned.

        existing_data_behavior (str): Specifies how to handle existing data when writing to the target.
            The default is "delete_matching", which deletes any data that matches the new write operation.

    Methods:
        __post_init__() -> None: Ensures that the partition columns are set correctly if the file format is
            FileFormat.PARQUET_PARTITIONED. If so, the default partitioning column is set to "category".

    Inherited Attributes:
        phase (Phase): The lifecycle phase in which the writer is operating (e.g., DATAPREP, ANALYSIS, GENAI).
        stage (Stage): The service stage (e.g., DQA, CLEAN, FEATURE) during which the data is being written.
        io_stage (Stage): The stage to which the data is being written (e.g., INGEST, CLEANED).
        name (str): The name of the dataset being written (e.g., "reviews").
        data_structure (DataStructure): The structure of the dataset, such as DataStructure.PANDAS for pandas DataFrames.
        format (FileFormat): The file format of the dataset, such as FileFormat.PARQUET_PARTITIONED for partitioned Parquet files.
    """

    partition_cols: List[str] = field(default_factory=list)
    existing_data_behavior: str = "delete_matching"

    def __post_init__(self) -> None:
        if self.format == FileFormat.PARQUET_PARTITIONED:
            self.partition_cols: List[str] = field(default_factory=lambda: ["category"])


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(DataClass):
    """
    ServiceConfig encapsulates the configuration necessary to perform a service operation that involves
    reading from a source dataset, checking or reading from a target dataset, and writing to a target dataset
    in a specific phase and stage of the data pipeline.

    Attributes:
        source_reader (Reader): The reader responsible for reading data from the source dataset.
        target_reader (Reader): The reader responsible for checking or reading from the target dataset
            to determine if data already exists or has been processed.
        target_writer (Writer): The writer responsible for writing the processed data to the target dataset.

        phase (Phase): The lifecycle phase in which the service is operating, such as:
            - DATAPREP: Data preparation phase.
            - ANALYSIS: Data analysis phase.
            - GENAI: Generative AI phase.

        stage (Stage): The specific stage of the service within the phase, such as DQA (Data Quality Assessment),
            CLEAN (data cleaning), or FEATURE (feature engineering).

        force (bool): Optional flag to force execution, bypassing certain checks or validations (default is False).
            When set to True, the service will proceed regardless of existing data or other conditions.
    """

    source_reader: Reader
    target_reader: Reader
    target_writer: Writer
    phase: Phase
    stage: Stage
    force: bool = False
