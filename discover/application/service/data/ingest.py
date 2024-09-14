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
# Modified   : Saturday September 14th 2024 03:34:04 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from dataclasses import dataclass
from typing import Any

from discover.application.service.base.service import ApplicationService
from discover.domain.service.base.repo import Repo
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
    repo: Repo = McKinneyRepo(config_cls=Config)
    stage: Stage = Stage.RAW
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class IngestTargetDataConfig(DataConfig):
    repo: Repo = McKinneyRepo(config_cls=Config)
    stage: Stage = Stage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
#                         DATA INGESTION APPLICATION SERVICE                                       #
# ------------------------------------------------------------------------------------------------ #
class DataIngestionApplicationService(ApplicationService):
    def __init__(
        self,
        config_cls: type[IngestConfig] = IngestConfig,
        context_cls: type[Context] = Context,
    ) -> None:
        super().__init__(config_cls=config_cls, context_cls=context_cls)
        self._config = self._config_cls(
            source_data_config=IngestSourceDataConfig(),
            target_data_config=IngestTargetDataConfig(),
        )
        self._domain_service = DataIngestionDomainService(
            config=self._config, context=self._context
        )

    def run(self) -> Any:
        return self._domain_service.run()
