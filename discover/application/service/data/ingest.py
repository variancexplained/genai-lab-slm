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
# Modified   : Saturday September 14th 2024 06:48:26 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from typing import Any

from discover.application.base.service import ApplicationService
from discover.domain.base.repo import Repo
from discover.domain.service.data.ingest.config import IngestConfig
from discover.domain.service.data.ingest.service import DataIngestionDomainService
from discover.domain.value_objects.config import DataConfig
from discover.domain.value_objects.context import Context
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.config import Config
from discover.infra.storage.repo.mckinney import McKinneyRepo


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

    repo: Repo = McKinneyRepo(config_cls=Config)
    stage: Stage = Stage.RAW
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

    repo: Repo = McKinneyRepo(config_cls=Config)
    stage: Stage = Stage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
#                         DATA INGESTION APPLICATION SERVICE                                       #
# ------------------------------------------------------------------------------------------------ #
class DataIngestionApplicationService(ApplicationService):
    """
    Application service for running the data ingestion process.

    This service configures the source and target data for the ingestion pipeline and delegates
    the execution to the domain service.

    Attributes:
        config_cls (type[IngestConfig]): Class type for configuring the ingestion, defaulting to IngestConfig.
        context_cls (type[Context]): Class type for managing the service context, defaulting to Context.
        _config (IngestConfig): Configures source and target data configurations for the ingestion process.
        _domain_service (DataIngestionDomainService): Domain service responsible for running the ingestion.
    """

    def __init__(
        self,
        config_cls: type[IngestConfig] = IngestConfig,
        context_cls: type[Context] = Context,
    ) -> None:
        """
        Initializes the application service by configuring source and target data and
        setting up the domain service.

        Args:
            config_cls (type[IngestConfig], optional): Class type for ingestion configuration.
            context_cls (type[Context], optional): Class type for managing the service context.
        """
        super().__init__(config_cls=config_cls, context_cls=context_cls)
        self._config = self._config_cls(
            source_data_config=IngestSourceDataConfig(),
            target_data_config=IngestTargetDataConfig(),
        )
        self._domain_service = DataIngestionDomainService(
            config=self._config, context=self._context
        )

    def run(self) -> Any:
        """
        Executes the data ingestion process by calling the domain service.

        Returns:
            Any: The result of the data ingestion process from the domain service.
        """
        return self._domain_service.run()
