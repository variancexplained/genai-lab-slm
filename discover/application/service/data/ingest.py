#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/ingest.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 05:35:17 pm                                              #
# Modified   : Tuesday September 17th 2024 01:34:21 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass, field
from typing import Any

from discover.application.base.service import ApplicationService
from discover.domain.base.repo import Repo
from discover.domain.service.core.data import Reader, Writer
from discover.domain.service.data.ingest.service import DataIngestionDomainService
from discover.domain.value_objects.config import DataConfig, ServiceConfig
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.reader import ConfigReader
from discover.infra.repo.mckinney import McKinneyRepo


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestSourceDataConfig(DataConfig):
    """
    Configuration class for the source data in the data ingestion pipeline.

    Attributes:
        repo (Repo): Repository instance for accessing source data, defaulting to McKinneyRepo.
        stage (Stage): The stage of the data, defaulting to RAW.
        name (str): The name of the source data, defaulting to "reviews".
    """

    repo: Repo = McKinneyRepo(config_reader_cls=ConfigReader)
    stage: Stage = DataPrepStage.RAW
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetDataConfig(DataConfig):
    """
    Configuration class for the target data in the data ingestion pipeline.

    Attributes:
        repo (Repo): Repository instance for storing ingested data, defaulting to McKinneyRepo.
        stage (Stage): The stage of the data, defaulting to INGEST.
        name (str): The name of the target data, defaulting to "reviews".
    """

    repo: Repo = McKinneyRepo(config_reader_cls=ConfigReader)
    stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DataIngestionServiceConfig(ServiceConfig):
    """
    Configuration class for the data ingestion process.

    This class defines the configuration parameters required for the data ingestion stage
    in the pipeline. It includes source and target data configurations, partitioning columns,
    text column details, encoding sample rate, random seed, and data type mappings for the input data.

    Attributes:
    -----------
    stage : Stage
        The stage of the processing pipeline, which is set to INGEST.
    source_data_config : DataConfig
        Configuration object for the source data to be ingested.
    target_data_config : DataConfig
        Configuration object for the target data after ingestion.
    partition_cols : list
        A list of column names used for partitioning the data. Defaults to ["category"].
    text_column : str
        The name of the text column in the dataset. Defaults to "content".
    encoding_sample : float
        The proportion of the data to sample for encoding checks. Defaults to 0.01 (1% of the data).
    random_state : int
        Random seed used for random operations within the ingestion process. Defaults to 22.
    datatypes : dict
        A dictionary that specifies the expected data types for each column in the dataset.
        The default mapping includes:
            - "id": "string"
            - "app_id": "string"
            - "app_name": "string"
            - "category_id": "category"
            - "category": "category"
            - "author": "string"
            - "rating": "int16"
            - "content": "string"
            - "vote_count": "int64"
            - "vote_sum": "int64"
            - "date": "datetime64[ms]"
    """

    stage: Stage = DataPrepStage.INGEST
    reader: Reader = Reader(config=IngestSourceDataConfig())
    writer: Writer = Writer(config=IngestTargetDataConfig())
    partition_cols: list = field(default_factory=lambda: ["category"])
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


# ------------------------------------------------------------------------------------------------ #
#                         DATA INGESTION APPLICATION SERVICE                                       #
# ------------------------------------------------------------------------------------------------ #
class DataIngestionApplicationService(ApplicationService):
    """
    Application service for orchestrating the data ingestion process.

    This service initializes the configuration for both source and target data, and delegates
    the execution of the ingestion to the domain service.

    Attributes:
    -----------
    _domain_service : DataIngestionDomainService
        The domain service responsible for executing the core data ingestion logic.
    """

    def __init__(
        self,
        config: DataIngestionServiceConfig = DataIngestionServiceConfig(),
    ) -> None:
        """
        Initializes the data ingestion application service with the provided configuration.

        This method sets up the configuration for the ingestion process and initializes the
        domain service that will handle the ingestion execution.

        Parameters:
        -----------
        config : DataIngestionServiceConfig, optional
            The configuration object that specifies the source and target data settings for the ingestion.
            Defaults to `DataIngestionServiceConfig`.
        """
        super().__init__(config=config)

        self._domain_service = DataIngestionDomainService(config=self._config)

    def run(self) -> Any:
        """
        Executes the data ingestion process by calling the domain service.

        Returns:
        --------
        Any:
            The result of the data ingestion process as executed by the domain service.
        """
        return self._domain_service.run()
