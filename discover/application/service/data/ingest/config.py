#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/ingest/config.py                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 12:24:56 am                                           #
# Modified   : Friday September 20th 2024 01:03:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  Ingestion Application Service Config"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Type

from discover.domain.base.repo import Repo
from discover.domain.entity.config.dataset import DatasetConfig
from discover.domain.entity.config.service import ServiceConfig
from discover.domain.entity.config.task import TaskConfig
from discover.domain.entity.context.service import ServiceContext
from discover.domain.entity.task import Task
from discover.domain.task.data.ingest import (
    CastDataTypeTask,
    RemoveNewlinesTask,
    VerifyEncodingTask,
)
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage
from discover.infra.repo.base import ReviewRepo
from discover.infra.repo.factory import ReviewRepoFactory

# ------------------------------------------------------------------------------------------------ #
repo_factory = ReviewRepoFactory()


# ------------------------------------------------------------------------------------------------ #
#                                         CONTEXT                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestContext(ServiceContext):
    """
    Context specific to the ingest phase of the data preparation process.

    Attributes:
        phase (Phase): The current phase of the pipeline, default is Phase.DATAPREP.
        stage (Stage): The specific stage within the phase, default is DataPrepStage.INGEST.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST


# ------------------------------------------------------------------------------------------------ #
#                                      DATA CONFIG                                                 #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestSourceDatasetConfig(DatasetConfig):
    """
    Configuration class for managing the source data specifically for the ingest stage of the pipeline.

    This class extends `DatasetConfig` and provides default values for the ingest process, such as the
    stage of data preparation and the name of the dataset.

    Attributes:
    -----------
    service_context : IngestContext, default=IngestContext()
        The execution context for the ingest process, providing phase and stage information specific to ingest tasks.

    stage : Stage, default=DataPrepStage.RAW
        The stage of the data preparation process. Defaults to `RAW`, indicating the raw data stage.

    name : str, default="reviews"
        The name of the dataset being ingested. Defaults to "reviews", commonly used for ingesting app reviews data.
    """

    service_context: ServiceContext = IngestContext()
    stage: Stage = DataPrepStage.RAW
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetDatasetConfig(DatasetConfig):
    """
    Configuration class for managing the target data specifically for the ingest stage of the pipeline.

    This class extends `DatasetConfig` and provides default values for the target data configuration in the
    ingest process, such as the stage of data preparation and the name of the dataset.

    Attributes:
    -----------
    service_context : IngestContext, default=IngestContext()
        The execution context for the ingest process, providing phase and stage information specific to ingest tasks.

    stage : Stage, default=DataPrepStage.INGEST
        The stage of the data preparation process, set to `INGEST`, indicating that this configuration is for
        the ingest stage of the pipeline.

    name : str, default="reviews"
        The name of the dataset being ingested and stored. Defaults to "reviews", typically used for storing
        app reviews data.
    """

    service_context: ServiceContext = IngestContext()
    stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
#                                INGEST TASK CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTaskConfig(TaskConfig):
    """
    Configuration class for managing task-specific settings during the ingest process.

    This class extends `TaskConfig` and adds additional configurations for ingest tasks, such as the
    service context specific to the ingest process and the text column that will be processed.

    Attributes:
    -----------
    service_context : IngestContext, default=IngestContext()
        The context in which the ingest task operates, providing information about the phase and stage
        of the ingest process.

    text_column : str, default="content"
        The name of the column in the dataset that contains the text data to be processed during the ingest task.
        Defaults to "content".

    Methods:
    --------
    validate() -> None:
        Validates the configuration by checking that `service_context` is an instance of `IngestContext` and that
        `text_column` is a string. Calls the base `TaskConfig` validation to ensure other aspects of the configuration
        are valid. Raises an `InvalidConfigException` if any of the validations fail.
    """

    service_context: IngestContext = IngestContext()
    text_column: str = "content"

    def _validate(self) -> list:
        """
        Validates the IngestTaskConfig.

        Ensures that:
        - `service_context` is an instance of `IngestContext`.
        - `text_column` is a valid string.

        It also calls the base `validate()` method from `TaskConfig` to perform any additional task-related validation.
        If any validation fails, an `InvalidConfigException` is raised with a descriptive error message.

        Raises:
        -------
        InvalidConfigException:
            If `service_context` is not an `IngestContext` or `text_column` is not a valid string, or if the base
            validation fails.
        """
        errors = super()._validate()

        if not isinstance(self.service_context, IngestContext):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected an IngestContext for service_context. Encountered {type(self.service_context).__name__}."
            )
        if not isinstance(self.text_column, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string for text_column. Encountered {type(self.text_column).__name__}."
            )

        return errors


# ------------------------------------------------------------------------------------------------ #
@dataclass
class RemoveNewLinesTaskConfig(IngestTaskConfig):
    """
    Configuration class for the task that removes new lines from the text.

    Attributes:
        service_context : IngestContext, default=IngestContext()
            The execution context for the ingest process, providing phase and stage information specific to ingest tasks.

        task (type[Task]): The specific task type, default is RemoveNewlinesTask.
    """

    task: Type[Task] = RemoveNewlinesTask


# ------------------------------------------------------------------------------------------------ #
@dataclass
class VerifyEncodingTaskConfig(IngestTaskConfig):
    """
    Configuration class for verifying the encoding of a sample of data during the ingest process.

    This class extends `IngestTaskConfig` and is used to configure a task that verifies the encoding of
    a portion of the data. The `encoding_sample` attribute determines the proportion of the data to sample,
    and `random_state` is used to ensure reproducibility of the sampling.

    Attributes:
    -----------
    task : Type[Task], default=VerifyEncodingTask
        The class type representing the task that verifies encoding. Defaults to `VerifyEncodingTask`.

    encoding_sample : float, default=0.2
        The proportion of the data to sample for encoding verification. A float between 0 and 1, where 1 indicates
        the entire dataset and 0.2 means 20% of the data will be sampled.

    random_state : int, default=55
        A seed value for random number generation to ensure reproducibility of the sampling process. Can be set
        to an integer value or left as `None` for random behavior.

    Methods:
    --------
    validate() -> None:
        Validates the configuration by checking that `encoding_sample` is a number (int or float) and that
        `random_state` is either an integer or `None`. Calls the base `validate()` method from `IngestTaskConfig`
        for additional validation. Raises an `InvalidConfigException` if any validation fails.
    """

    task: Type[Task] = VerifyEncodingTask
    encoding_sample: float = 0.2
    random_state: int = 55

    def _validate(self) -> list:
        """
        Validates the VerifyEncodingTaskConfig.

        This method checks:
        - `encoding_sample` is a number (int or float).
        - `random_state` is either an integer or `None`.

        It also invokes the base `validate()` method from `IngestTaskConfig` to ensure that other task-specific
        configurations are valid. If any validation fails, an `InvalidConfigException` is raised with a detailed
        error message.

        Raises:
        -------
        InvalidConfigException:
            If `encoding_sample` is not a valid number or `random_state` is not an integer or `None`, or if
            any base validation fails.
        """
        errors = super()._validate()

        if not isinstance(self.encoding_sample, (int, float)):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a number for encoding_sample. Encountered {type(self.encoding_sample).__name__}."
            )

        if self.random_state and not isinstance(self.random_state, int):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected an integer or None for random_state. Encountered {type(self.random_state).__name__}."
            )

        return errors


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CastDataTypeTaskConfig(IngestTaskConfig):
    """
    Configuration class for casting dataset columns to specific data types during the ingest process.

    This class extends `IngestTaskConfig` and is used to configure a task that casts data columns to the
    appropriate data types, as specified in the `datatypes` attribute.

    Attributes:
    -----------
    task : Type[Task], default=CastDataTypeTask
        The class type representing the task responsible for casting data columns to their specified types.
        Defaults to `CastDataTypeTask`.

    datatypes : dict
        A dictionary mapping column names to their respective data types. The default includes commonly used
        columns like `id`, `app_id`, `rating`, and others, with their corresponding data types such as `string`,
        `int16`, `category`, and `datetime64[ms]`.

    Methods:
    --------
    validate() -> None:
        Validates the configuration by ensuring that the `datatypes` dictionary is provided and contains
        valid mappings. Calls the base `IngestTaskConfig` validation to ensure other configurations are valid.
        Raises an `InvalidConfigException` if any validation fails.
    """

    task: Type[Task] = CastDataTypeTask
    datatypes: dict = field(
        default_factory=lambda: {
            "id": "string",
            "app_id": "string",
            "app_name": "string",
            "category_id": "category",
            "category": "category",
            "author": "string",
            "rating": "int16",
            "content": "string",
            "vote_count": "int64",
            "vote_sum": "int64",
            "date": "datetime64[ms]",
        }
    )

    def _validate(self) -> list:
        """
        Validates the CastDataTypeTaskConfig.

        Ensures that the `datatypes` attribute is a non-empty dictionary mapping column names to their respective
        data types. Also calls the base `validate()` method from `IngestTaskConfig` to perform additional
        task-related validation. If validation fails, an `InvalidConfigException` is raised.

        Raises:
        -------
        InvalidConfigException:
            If `datatypes` is empty or invalid, or if any base validation fails.
        """
        errors = super()._validate()

        if not self.datatypes:
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a non-empty dictionary containing a datatype mapping."
            )

        return errors


# ------------------------------------------------------------------------------------------------ #
#                                INGEST SERVICE CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestServiceConfig(ServiceConfig):
    """
    Configuration class for managing the service-specific settings in the ingest process.

    This class extends `ServiceConfig` and provides configurations for the ingest process, such as
    repository handling, source and target data configurations, task-specific configurations, and the
    optional `force` flag to re-execute tasks even if they were previously completed.

    Attributes:
    -----------
    service_context : ServiceContext, default=IngestContext()
        The context for the ingest service, including metadata about the phase and stage of execution.

    repo : Optional[Repo], default=None
        The repository used for managing data in the ingest process. The repository is automatically initialized
        using the `source_data_config` during the post-initialization step.

    source_data_config : DatasetConfig, default=IngestSourceDatasetConfig()
        Configuration for the source data, defining how data is ingested.

    target_data_config : DatasetConfig, default=IngestTargetDatasetConfig()
        Configuration for the target data, defining how data is stored after processing.

    task_configs : List[TaskConfig], default=[]
        A list of task configurations defining the specific tasks to be executed during the ingest process.
        Default tasks such as `RemoveNewLinesTaskConfig`, `VerifyEncodingTaskConfig`, and `CastDataTypeTaskConfig`
        are added during the post-initialization step.

    force : bool, default=False
        A flag indicating whether to force re-execution of tasks, even if they have been completed previously.

    Methods:
    --------
    __post_init__() -> None:
        Initializes the repository and appends default task configurations to the `task_configs` list.

    validate() -> None:
        Validates the ingest service configuration, ensuring that the repository, task configurations,
        and data configurations are correctly set and valid. Raises an `InvalidConfigException` if any validation fails.
    """

    service_context: ServiceContext = IngestContext()
    repo: Optional[Repo] = None
    source_data_config: DatasetConfig = IngestSourceDatasetConfig()
    target_data_config: DatasetConfig = IngestTargetDatasetConfig()
    task_configs: List[TaskConfig] = field(default_factory=list)
    force: bool = False

    def __post_init__(self) -> None:
        """
        Post-initialization method for the ingest service configuration.

        This method initializes the repository using the `source_data_config` and appends default task configurations
        for removing new lines, verifying encoding, and casting data types to the `task_configs` list.
        """
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.repo = repo_factory.get_repo(config=self.source_data_config)
        self.task_configs.append(RemoveNewLinesTaskConfig())
        self.task_configs.append(VerifyEncodingTaskConfig())
        self.task_configs.append(CastDataTypeTaskConfig())
        super().__post_init__()

    def _validate(self) -> list:
        """
        Validates the IngestServiceConfig.

        Ensures that:
        - `repo` is a valid instance of `ReviewRepo`.
        - `task_configs` is not empty and contains valid task configurations.
        - The service context, source data configuration, and target data configuration are valid.

        Calls the `validate()` method on each of the task configurations to ensure they are valid.
        If any validation fails, an `InvalidConfigException` is raised with a detailed error message.

        Raises:
        -------
        InvalidConfigException:
            If the repository, task configurations, or data configurations are invalid or missing.
        """
        errors = super()._validate()

        if not isinstance(self.repo, ReviewRepo):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a ReviewRepo subclass instance. Encountered {type(self.repo).__name__}."
            )

        if not self.task_configs:
            errors.append(
                f"Invalid {self.__class__.__name__}. Tasks have not been configured."
            )
        return errors
