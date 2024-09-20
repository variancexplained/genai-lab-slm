#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/ingest/pipeline.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 12:25:06 am                                           #
# Modified   : Thursday September 19th 2024 09:14:41 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


from typing import Any

from pandarallel import pandarallel

from discover.application.ops.announcer import pipeline_announcer
from discover.application.service.base.pipeline import Pipeline
from discover.domain.entity.config.service import ServiceConfig

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
class IngestPipeline(Pipeline):
    """
    IngestPipeline orchestrates the data ingestion process by reading from the source, executing
    a sequence of tasks (such as data transformations or validations), and writing the final data
    to the target destination. This pipeline is designed to be flexible and extendable, allowing
    tasks to be added to the pipeline and executed in sequence.

    Args:
        config (ServiceConfig): Configuration object that provides the source reader, target reader,
            target writer, and other necessary parameters for the pipeline.

    Methods:
        _run_pipeline() -> Any:
            Orchestrates the execution of the pipeline by reading data from the source,
            running each task in sequence on the data, and writing the processed data to the target.
    """

    def __init__(self, config: ServiceConfig) -> None:
        super().__init__(config=config)

    @pipeline_announcer
    def _run_pipeline(self) -> Any:
        """
        Orchestrates the execution of the data ingestion pipeline.

        This method reads data from the source, executes each task in sequence on the data,
        and writes the processed data to the target destination. The final processed data is returned.

        Returns:
        --------
        Any:
            The final processed data after executing all tasks in the pipeline.
        """
        data = self._repo.get(
            stage=self._config.source_data_config.stage,
            name=self._config.source_data_config.name,
        )

        for task in self._tasks:
            data = self._run_task(data=data, task=task)

        self._repo.add(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
            data=data,
        )
        return data
