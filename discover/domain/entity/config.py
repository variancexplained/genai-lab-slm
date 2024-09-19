#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/config.py                                                   #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday September 10th 2024 04:49:55 pm                                             #
# Modified   : Thursday September 19th 2024 02:14:24 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Abstract Base Class for Data Processing Stage Configurations"""
from __future__ import annotations

import logging
from abc import abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Type

from discover.core.data import DataClass
from discover.domain.entity.context import ServiceContext
from discover.domain.entity.task import Task
from discover.domain.exception.config import InvalidConfigException
from discover.domain.value_objects.data_structure import DataStructure
from discover.domain.value_objects.file import ExistingDataBehavior, FileFormat
from discover.domain.value_objects.lifecycle import Phase, Stage


# ------------------------------------------------------------------------------------------------ #
#                                    CONFIG                                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Config(DataClass):
    """
    Base configuration class that defines the structure for validating configuration objects.

    Subclasses must implement the `validate` method to enforce specific validation logic for the configuration.

    Methods:
        validate() -> None: Validates the configuration. Raises an exception if the configuration is invalid.
    """

    @abstractmethod
    def validate(self) -> None:
        """Validates the configuration, raising an exception if invalid."""
        pass


# ------------------------------------------------------------------------------------------------ #
#                                  DATA CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataConfig(Config):
    """
    Configuration class for handling data-related tasks within a pipeline.

    This class defines various configurations necessary for processing data in different stages
    of the pipeline, such as lifecycle phases, dataset structure, file formats, and partitioning options.
    It also manages behaviors related to existing data when processing or writing datasets.

    Attributes:
    -----------
    phase : Phase
        The lifecycle phase, e.g., DATAPREP, ANALYSIS, or GENAI, indicating the current stage
        of the data processing lifecycle.

    stage : Stage
        The specific stage of the service pipeline (e.g., DQA, CLEAN, FEATURE) during which
        this configuration applies.

    name : str
        The name of the dataset being processed or created.

    data_structure : DataStructure, default=DataStructure.PANDAS
        Defines the structure of the dataset, such as whether it is a Pandas DataFrame or another
        data structure (e.g., Spark DataFrame).

    format : FileFormat, default=FileFormat.PARQUET_PARTITIONED
        Specifies the file format of the dataset, e.g., Parquet partitioned or other formats.

    partition_cols : List[str], default=[]
        Columns by which the dataset is partitioned when saved, used primarily for partitioned file
        formats like Parquet.

    existing_data_behavior : Optional[ExistingDataBehavior], default=None
        Defines the behavior when existing data is encountered, such as whether to overwrite or delete
        matching records.

    Methods:
    --------
    __post_init__() -> None:
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        if the file format is `PARQUET_PARTITIONED`. Ensures that partitioning is applied correctly based
        on the dataset format.

    validate() -> None:
        Validates the configuration, ensuring that all required attributes are correctly set.

        This method performs type checks on the attributes `phase`, `stage`, `name`, `data_structure`, `format`,
        `partition_cols`, and `existing_data_behavior`. It raises an `InvalidConfigException` if any attribute
        is invalid, with a clear message indicating the invalid attribute and its expected type.

        Logs an error message before raising the exception, allowing for easier debugging.
    """

    phase: Phase  # Lifecycle phase, i.e. DATAPREP, ANALYSIS, or GENAI
    stage: Stage  # The Stage to/from the data will be written/read.
    name: str  # The name of the dataset
    data_structure: DataStructure = DataStructure.PANDAS  # The dataset data structure.
    format: FileFormat = FileFormat.PARQUET_PARTITIONED  # The dataset file format.
    partition_cols: List[str] = field(default_factory=list)
    existing_data_behavior: Optional[ExistingDataBehavior] = None

    def __post_init__(self) -> None:
        """
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        if the file format is `PARQUET_PARTITIONED`. This ensures partitioning is correctly applied for
        partitioned datasets.
        """
        if self.format == FileFormat.PARQUET_PARTITIONED:
            self.partition_cols: List[str] = field(default_factory=lambda: ["category"])
            self.existing_data_behavior: str = "delete_matching"

    def validate(self) -> None:
        errors = []
        if not isinstance(self.phase, Phase):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Phase instance. Encountered {type(self.phase).__name__}."
            )
        if not isinstance(self.stage, Stage):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Stage instance. Encountered {type(self.stage).__name__}."
            )
        if not isinstance(self.name, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string for name. Encountered {type(self.name).__name__}."
            )
        if not isinstance(self.data_structure, DataStructure):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DataStructure instance. Encountered {type(self.data_structure).__name__}."
            )
        if not isinstance(self.format, FileFormat):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a FileFormat instance. Encountered {type(self.format).__name__}."
            )
        if not isinstance(self.partition_cols, list):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a list for partition_cols. Encountered {type(self.partition_cols).__name__}."
            )
        if self.existing_data_behavior and not isinstance(
            self.existing_data_behavior, ExistingDataBehavior
        ):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected an ExistingDataBehavior instance. Encountered {type(self.existing_data_behavior).__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)


# ------------------------------------------------------------------------------------------------ #
#                                   TASK CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class TaskConfig(Config):
    """
    Configuration class for defining a task within a pipeline.

    This class holds the basic configuration for a task, including the lifecycle phase,
    the stage of the task in the pipeline, and the task class type itself.

    Attributes:
    -----------
    phase : Phase
        The lifecycle phase during which the task is executed (e.g., DATAPREP, ANALYSIS).

    stage : Stage
        The specific stage of the task within the phase (e.g., INGEST, CLEAN, DQA).

    task : Type[Task]
        A Task class type representing the task to be executed. Must be a subclass of `Task`.

    Methods:
    --------
    validate() -> None:
        Validates the task configuration, checking if the phase, stage, and task type are correctly set.
        Raises an `InvalidConfigException` if any of the values are invalid or missing.
    """

    phase: Phase
    stage: Stage
    task: Type[Task]

    def validate(self) -> None:
        """
        Validates the task configuration.

        Ensures that `phase` is an instance of `Phase`, `stage` is an instance of `Stage`,
        and `task` is a subclass of `Task`. Raises an `InvalidConfigException` with a detailed
        error message if any of the checks fail.
        """
        errors = []
        if not isinstance(self.phase, Phase):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Phase instance. Encountered {type(self.phase).__name__}."
            )
        if not isinstance(self.stage, Stage):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a Stage instance. Encountered {type(self.stage).__name__}."
            )
        if not issubclass(self.task, Task):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a subclass of Task. Encountered {self.task.__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)


# ------------------------------------------------------------------------------------------------ #
#                                       SERVICE CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceConfig(Config):
    """
    Configuration class for defining service-specific settings within the pipeline.

    This class manages the execution context, source data configuration, target data configuration,
    and an optional `force` flag that determines if the service should re-execute, even if previous tasks
    were completed.

    Attributes:
    -----------
    context : ServiceContext
        The execution context for the service, containing phase and stage metadata.

    source_data_config : DataConfig
        The configuration for the source data, defining how the input data will be handled.

    target_data_config : DataConfig
        The configuration for the target data, specifying how and where the processed data will be stored.

    force : bool, default=False
        A flag indicating whether to force the service to re-execute tasks, even if they were already completed.

    Methods:
    --------
    validate() -> None:
        Validates the service configuration, ensuring that the context, source data, and target data configurations
        are correctly set. It also validates that the `force` attribute is a boolean. Raises an `InvalidConfigException`
        if any attribute is invalid or misconfigured.
    """

    context: ServiceContext
    source_data_config: DataConfig
    target_data_config: DataConfig
    force: bool = False

    def validate(self) -> None:
        """
        Validates the service configuration.

        Ensures that:
        - `context` is an instance of `ServiceContext`.
        - `source_data_config` and `target_data_config` are instances of `DataConfig`.
        - `force` is a boolean value.

        Additionally, it validates the `context`, `source_data_config`, and `target_data_config`
        by calling their respective `validate()` methods.

        Raises:
        -------
        InvalidConfigException:
            If any of the attributes are invalid, the error messages are collected, logged, and raised as an exception.
        """
        errors = []

        if not isinstance(self.context, ServiceContext):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a ServiceContext instance. Encountered {type(self.context).__name__}."
            )
        if not isinstance(self.source_data_config, DataConfig):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DataConfig instance. Encountered {type(self.source_data_config).__name__}."
            )
        if not isinstance(self.target_data_config, DataConfig):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a DataConfig instance. Encountered {type(self.target_data_config).__name__}."
            )
        if not isinstance(self.force, bool):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a boolean type. Encountered {type(self.stage).__name__}."
            )

        # Ensure members are valid
        self.context.validate()
        self.source_data_config.validate()
        self.target_data_config.validate()

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)
