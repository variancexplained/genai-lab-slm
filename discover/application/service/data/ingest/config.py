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
# Modified   : Wednesday September 18th 2024 05:26:40 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  Ingestion Application Service Config"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from discover.application.service.base.config import DataConfig, ServiceConfig
from discover.domain.base.repo import Repo
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage
from discover.infra.repo.factory import ReviewRepoFactory

# ------------------------------------------------------------------------------------------------ #
repo_factory = ReviewRepoFactory()


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


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetReaderConfig(DataConfig):
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
@dataclass
class IngestServiceConfig(ServiceConfig):
    """"""

    repo: Optional[Repo] = None
    source_data_config: DataConfig = IngestSourceDataConfig()
    target_data_config: DataConfig = IngestTargetReaderConfig()
    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    text_column: str = "content"
    encoding_sample: float = 0.01
    random_state: int = 22
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
    force: bool = False

    def __post_init__(self) -> None:
        self.repo = repo_factory.get_repo(config=self.source_data_config)
