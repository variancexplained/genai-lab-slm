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
# Modified   : Thursday September 19th 2024 02:37:23 pm                                            #
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
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
#                                    CONFIG                                                        #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class Config(DataClass):
    """
    Base configuration class for services within the pipeline.

    This class acts as a foundation for more specific configuration classes and ensures that
    the essential `service_context` is valid and properly configured.

    Attributes:
    -----------
    service_context : ServiceContext
        The execution context for the service, containing metadata about the current phase and stage.

    Methods:
    --------
    validate() -> None:
        Validates the configuration by ensuring that `service_context` is an instance of `ServiceContext`.
        Calls the `validate()` method of the `service_context` to ensure that its internal data is also valid.
        Raises an `InvalidConfigException` if any validation checks fail.
    """

    service_context: ServiceContext

    @abstractmethod
    def validate(self) -> None:
        """
        Validates the base configuration.

        Ensures that `service_context` is an instance of `ServiceContext` and calls its `validate()` method
        to perform deeper validation. If `service_context` is invalid or misconfigured, an `InvalidConfigException`
        is raised with an appropriate error message.

        Raises:
        -------
        InvalidConfigException:
            If `service_context` is not a valid instance of `ServiceContext`, or if the validation of the
            `service_context` fails.
        """
        errors = []
        if not isinstance(self.service_context, ServiceContext):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a ServiceContext instance. Encountered {type(self.service_context).__name__}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)

        # Validate the service context itself
        self.service_context.validate()


# ------------------------------------------------------------------------------------------------ #
#                                  DATA CONFIG                                                     #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataConfig(Config):
    """
    Configuration class for managing the dataset's structure, format, and behavior in a data pipeline.

    This class defines various configurations required for reading or writing data in different stages
    of the pipeline, such as dataset structure, file format, and behavior when encountering existing data.

    Attributes:
    -----------
    stage : Stage
        The specific stage of the pipeline where the data will be read or written (e.g., INGEST, TRANSFORM).

    name : str
        The name of the dataset being processed or created.

    data_structure : DataStructure, default=DataStructure.PANDAS
        Defines the structure of the dataset, such as whether it is a Pandas DataFrame or another data structure
        (e.g., Spark DataFrame).

    format : FileFormat, default=FileFormat.PARQUET_PARTITIONED
        Specifies the file format of the dataset, e.g., Parquet partitioned or other formats.

    partition_cols : List[str], default=[]
        The list of columns by which the dataset is partitioned when saved, primarily for use with partitioned
        file formats like Parquet.

    existing_data_behavior : Optional[ExistingDataBehavior], default=None
        Defines the behavior when encountering existing data, such as whether to overwrite or delete
        matching records.

    Methods:
    --------
    __post_init__() -> None:
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        when the file format is `PARQUET_PARTITIONED`. Ensures that the partitioning behavior is applied
        correctly when working with partitioned datasets.

    validate() -> None:
        Validates the configuration by checking that `stage`, `name`, `data_structure`, `format`,
        `partition_cols`, and `existing_data_behavior` (if provided) are correctly set. Inherits the base
        validation from `Config` and raises an `InvalidConfigException` if any attribute is invalid.
    """

    stage: Stage  # The Stage to/from the data will be written/read.
    name: str  # The name of the dataset
    data_structure: DataStructure = DataStructure.PANDAS  # The dataset data structure.
    format: FileFormat = FileFormat.PARQUET_PARTITIONED  # The dataset file format.
    partition_cols: List[str] = field(default_factory=list)
    existing_data_behavior: Optional[str] = None

    def __post_init__(self) -> None:
        """
        A post-initialization hook that sets default values for `partition_cols` and `existing_data_behavior`
        when the file format is `PARQUET_PARTITIONED`. This ensures that partitioning columns and data behavior
        are applied correctly when working with partitioned datasets.
        """
        if self.format == FileFormat.PARQUET_PARTITIONED:
            self.partition_cols = ["category"]
            self.existing_data_behavior = ExistingDataBehavior.DELETE_MATCHING.value

    def validate(self) -> None:
        """
        Validates the DataConfig.

        Inherits the base validation from `Config` to ensure the service context is valid. Additionally, this method
        checks:
        - `stage` is an instance of `Stage`.
        - `name` is a string.
        - `data_structure` is an instance of `DataStructure`.
        - `format` is an instance of `FileFormat`.
        - `partition_cols` is a list.
        - `existing_data_behavior` is either `None` or an instance of `ExistingDataBehavior`.

        If any of these checks fail, an `InvalidConfigException` is raised with a detailed error message.
        """
        super().validate()
        errors = []
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
    Configuration class for defining a task within the pipeline.

    This class holds the configuration for a task, ensuring that the task is a valid subclass of `Task`.
    It extends the base `Config` class, inheriting its validation logic while adding task-specific checks.

    Attributes:
    -----------
    task : Type[Task]
        A Task class type representing the task to be executed. The class must be a subclass of `Task`.

    Methods:
    --------
    validate() -> None:
        Validates the task configuration by ensuring that `task` is a valid subclass of `Task`.
        It inherits the base validation from `Config` and raises an `InvalidConfigException`
        if the task type is invalid.
    """

    task: Type[Task]

    def validate(self) -> None:
        """
        Validates the TaskConfig.

        This method performs validation to ensure that `task` is a subclass of `Task`. If `task` is
        not a subclass of `Task`, an `InvalidConfigException` is raised with a descriptive error message.
        It also calls the `validate()` method of the base `Config` class to ensure that any other
        required configuration elements are valid.

        Raises:
        -------
        InvalidConfigException:
            If `task` is not a valid subclass of `Task`, or if the base configuration fails validation.
        """
        super().validate()
        errors = []

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
    Configuration class for managing service-related settings in the pipeline.

    This class handles the configuration of source and target data within the service and includes
    an optional flag, `force`, which determines whether tasks should be re-executed even if they
    were completed previously.

    Attributes:
    -----------
    source_data_config : DataConfig
        The configuration for the source data, defining how the input data is handled.

    target_data_config : DataConfig
        The configuration for the target data, specifying where and how the processed data will be stored.

    force : bool, default=False
        A flag indicating whether to force the re-execution of tasks, even if they have been previously completed.

    Methods:
    --------
    validate() -> None:
        Validates the service configuration by ensuring that `source_data_config` and `target_data_config`
        are valid instances of `DataConfig`, and that `force` is a boolean value. It also validates
        the `source_data_config` and `target_data_config` by invoking their respective `validate()` methods.
        Raises an `InvalidConfigException` if any attribute is invalid.
    """

    source_data_config: DataConfig
    target_data_config: DataConfig
    force: bool = False

    def validate(self) -> None:
        """
        Validates the ServiceConfig.

        This method checks:
        - `source_data_config` is an instance of `DataConfig`.
        - `target_data_config` is an instance of `DataConfig`.
        - `force` is a boolean value.

        It also calls the `validate()` method of both `source_data_config` and `target_data_config` to ensure
        they are valid configurations themselves. If any validation fails, an `InvalidConfigException` is raised
        with a detailed error message.

        Raises:
        -------
        InvalidConfigException:
            If any of the configuration attributes are invalid, or if validation of the `source_data_config`
            or `target_data_config` fails.
        """
        super().validate()
        errors = []

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
                f"Invalid {self.__class__.__name__}. Expected a boolean type. Encountered {type(self.force).__name__}."
            )

        # Validate the source and target configs
        self.source_data_config.validate()
        self.target_data_config.validate()

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)
