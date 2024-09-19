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
# Modified   : Thursday September 19th 2024 02:17:04 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  Ingestion Application Service Config"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from discover.domain.base.repo import Repo
from discover.domain.entity.config import DataConfig, ServiceConfig, TaskConfig
from discover.domain.entity.context import ServiceContext
from discover.domain.entity.task import Task
from discover.domain.task.data.ingest import (
    CastDataTypeTask,
    RemoveNewlinesTask,
    VerifyEncodingTask,
)
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage
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
class IngestSourceDataConfig(DataConfig):
    """
    Configuration class for ingesting source data in the data preparation phase.

    This class extends `DataConfig` and provides specific default values for the ingestion
    of source data, such as the lifecycle phase (`DATAPREP`), the stage (`RAW`), and the
    default dataset name (`reviews`). It is designed to configure the source data for ingestion
    tasks during the data preparation pipeline.

    Attributes:
    -----------
    phase : Phase, default=Phase.DATAPREP
        The lifecycle phase of the data pipeline, set to `DATAPREP` by default, representing
        the data preparation phase of the workflow.

    stage : Stage, default=DataPrepStage.RAW
        The stage of the data preparation process, set to `RAW`, indicating that the source
        data is in its raw, unprocessed form.

    name : str, default="reviews"
        The name of the dataset being ingested, set to "reviews" as a default for ingesting
        reviews data.

    Inherits:
    ---------
    DataConfig:
        Inherits all attributes and methods from the `DataConfig` class, including configuration
        for data structures, file formats, and partitioning options.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.RAW
    name: str = "reviews"

    def validate(self) -> None:
        msg = None
        if not isinstance(self.phase, Phase):
            msg = f"Invalid {self.__class__.__name__}. Expected a Phase type. Encountered {type[self.phase].__name__}"
        if not isinstance(self.stage, DataPrepStage):
            msg = f"Invalid {self.__class__.__name__}. Expected a Stage type. Encountered {type[self.stage].__name__}"
        if not isinstance(self.name, str):
            msg = f"Invalid {self.__class__.__name__}. Expected a string type. Encountered {type[self.name].__name__}"
        if msg:
            logging.exception(msg)
            raise TypeError(msg)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetDataConfig(DataConfig):
    """
    Configuration class for reading target data during the ingestion phase of data preparation.

    This class extends `ReaderConfig` and sets default values specific to the ingestion
    of target data, such as the lifecycle phase (`DATAPREP`), the stage (`INGEST`), and
    the default dataset name (`reviews`). It is designed to configure how target data
    should be read during the ingestion phase of the data preparation pipeline.

    Attributes:
    -----------
    phase : Phase, default=Phase.DATAPREP
        The lifecycle phase of the data pipeline, set to `DATAPREP` by default, indicating
        that this configuration is used during the data preparation phase.

    stage : Stage, default=DataPrepStage.INGEST
        The stage of the data preparation process, set to `INGEST`, specifying that the data
        reading happens during the ingestion stage.

    name : str, default="reviews"
        The name of the dataset being read, set to "reviews" as a default for target data ingestion.

    Inherits:
    ---------
    ReaderConfig:
        Inherits all attributes and methods from the `ReaderConfig` class, which defines
        the general configuration for reading data.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
#                                INGEST TASK CONFIG                                                #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTaskConfig(TaskConfig):
    """
    Configuration class for tasks within the ingest phase of the data pipeline.

    Attributes:
        context (IngestContext): The context specific to the ingest phase, providing phase and stage information.
        task (type[Task]): The specific task type, default is Task.
        text_column (str): The name of the column that contains text data to be processed, default is "content".
    """

    context: IngestContext = IngestContext()
    task: type[Task] = Task
    text_column: str = "content"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class RemoveNewLinesTaskConfig(IngestTaskConfig):
    """
    Configuration class for the task that removes new lines from the text.

    Attributes:
        task (type[Task]): The specific task type, default is RemoveNewlinesTask.
    """

    task: type[Task] = RemoveNewlinesTask


# ------------------------------------------------------------------------------------------------ #
@dataclass
class VerifyEncodingTaskConfig(IngestTaskConfig):
    """
    Configuration class for verifying the encoding of text data.

    Attributes:
        task (type[Task]): The specific task type, default is VerifyEncodingTask.
        encoding_sample (float): The proportion of the dataset to sample for encoding verification, default is 0.2.
        random_state (int): The random seed used for sampling, default is 55.
    """

    task: type[Task] = VerifyEncodingTask
    encoding_sample: float = 0.2
    random_state: int = 55


# ------------------------------------------------------------------------------------------------ #
@dataclass
class CastDataTypeTaskConfig(IngestTaskConfig):
    """
    Configuration class for casting columns to specific data types.

    Attributes:
        task (type[Task]): The specific task type, default is VerifyEncodingTask.
        datatypes (dict): A dictionary mapping column names to their respective data types.
    """

    task: type[Task] = CastDataTypeTask
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


# ------------------------------------------------------------------------------------------------ #
#                                INGEST SERVICE CONFIG                                             #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestServiceConfig(ServiceConfig):
    """
    Configuration class for the Ingest service, which orchestrates the execution of the ingest pipeline.

    Attributes:
        context (ServiceContext): The context object that tracks the current phase and stage.
        repo (Optional[Repo]): The repository object used for interacting with the source and target data.
        source_data_config (DataConfig): Configuration for the source data in the ingest pipeline.
        target_data_config (DataConfig): Configuration for the target data in the ingest pipeline.
        task_configs (List[TaskConfig]): A list of task configuration objects that define the tasks in the pipeline.
        force (bool): Flag to force the execution of tasks, even if they were previously completed, default is False.
    """

    context: ServiceContext = ServiceContext
    repo: Optional[Repo] = None
    source_data_config: DataConfig = IngestSourceDataConfig()
    target_data_config: DataConfig = IngestTargetDataConfig()
    task_configs: List[TaskConfig] = field(default_factory=list)
    force: bool = False

    def __post_init__(self) -> None:
        """
        Initializes the repository using the repo_factory and appends default task configurations
        for removing new lines, verifying encoding, and casting data types.
        """
        self.repo = repo_factory.get_repo(config=self.source_data_config)
        self.task_configs.append(RemoveNewLinesTaskConfig())
        self.task_configs.append(VerifyEncodingTaskConfig())
        self.task_configs.append(CastDataTypeTaskConfig())
