#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/ingest/pipeline.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Tuesday September 17th 2024 09:55:34 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Module"""


from typing import Any

from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.domain.base.pipeline import Pipeline, PipelineBuilder
from discover.domain.service.core.monitor.announcer import pipeline_announcer
from discover.domain.service.data.ingest.task import (
    CastDataTypeTask,
    RemoveNewlinesTask,
    VerifyEncodingTask,
)
from discover.domain.value_objects.config import ServiceConfig

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
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
        data = self._source_reader.read()

        for task in self._tasks:
            data = task.run(data)

        self._target_writer.write(data=data)
        return data


# ------------------------------------------------------------------------------------------------ #
#                                INGEST PIPELINE BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class IngestPipelineBuilder(PipelineBuilder):
    """
    Builder class for constructing the data ingestion pipeline.

    This class is responsible for creating a data ingestion pipeline by
    assembling the required tasks (e.g., reading, ingesting, writing)
    and returning a configured pipeline ready for execution. The pipeline
    handles tasks related to data ingestion and quality analysis.

    Methods:
    --------
    create_pipeline() -> IngestPipeline:
        Constructs the data ingestion pipeline, adds the relevant tasks,
        and returns the configured pipeline.
    """

    def __init__(
        self, config: ServiceConfig, pipeline_cls: type[Pipeline] = IngestPipeline
    ) -> None:
        """
        Initializes the IngestPipelineBuilder with the provided configuration and context.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object that contains settings and parameters for building the pipeline.
        context : Context
            Context object that tracks metadata related to the pipeline's execution.
        """
        super().__init__(config=config, pipeline_cls=pipeline_cls)

    def create_pipeline(self) -> IngestPipeline:
        """
        Creates and configures the ingestion pipeline with the necessary tasks.

        This method sets up the pipeline by adding tasks such as reading from the source,
        ingesting data, and writing to the target. It returns a fully constructed
        pipeline ready for execution.

        Returns:
        --------
        IngestPipeline:
            The fully configured data ingestion pipeline with all tasks.
        """
        # Instantiate pipeline
        pipe = self._pipeline_cls(config=self._config)

        newlines = RemoveNewlinesTask(config=self._config)
        encoding = VerifyEncodingTask(config=self._config)
        dtypes = CastDataTypeTask(config=self._config)

        # Add tasks to pipeline
        pipe.add_task(newlines)
        pipe.add_task(encoding)
        pipe.add_task(dtypes)

        return pipe
