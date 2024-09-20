#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/dqa/pipeline.py                                  #
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
"""Data Quality Assessment Pipeline Module"""
import warnings
from typing import Any

import fasttext
import pandas as pd
from pandarallel import pandarallel

from discover.application.ops.announcer import pipeline_announcer
from discover.application.service.base.pipeline import Pipeline, PipelineBuilder
from discover.application.service.data import dqa
from discover.domain.entity.config.service import ServiceConfig

# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=18, verbose=0)
# ------------------------------------------------------------------------------------------------ #
warnings.filterwarnings("ignore")
fasttext.FastText.eprint = lambda x: None

fasttext


# ------------------------------------------------------------------------------------------------ #
class DQAPipeline(Pipeline):
    """
    DQAPipeline orchestrates the data ingestion process by reading from the source, executing
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
        # Obtain source data
        data = self._repo.get(
            stage=self._config.source_data_config.stage,
            name=self._config.source_data_config.name,
        )
        # Initialize data with sorting by id
        data = self._initialize(data=data)
        # dqa_data will contain a DataFrame of indicator values for each row and
        # data quality aspect
        dqa_data = pd.DataFrame()

        for task in self._tasks:
            result = self._run_task(data=data, task=task)
            dqa_data = pd.concat([dqa_data, result.to_frame()], axis=1)

        # Concatenate data with quality indicators
        data = self._finalize(data=data, dqa_data=dqa)

        self._repo.add(
            stage=self._config.target_data_config.stage,
            name=self._config.target_data_config.name,
            data=data,
        )

        return data

    def _initialize(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.reset_index().set_index(keys=[self._config.init_sort_by])
        data = data.sort_index(ascending=True)
        return data.reset_index()

    def _finalize(self, data: pd.DataFrame, qa_data: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([data, qa_data], axis=1)


# ------------------------------------------------------------------------------------------------ #
#                                INGEST PIPELINE BUILDER                                            #
# ------------------------------------------------------------------------------------------------ #
class DQAPipelineBuilder(PipelineBuilder):
    """
    Builder class for constructing the data ingestion pipeline.

    This class is responsible for creating a data ingestion pipeline by
    assembling the required tasks (e.g., reading, ingesting, writing)
    and returning a configured pipeline ready for execution. The pipeline
    handles tasks related to data ingestion and quality analysis.

    Methods:
    --------
    create_pipeline() -> DQAPipeline:
        Constructs the data ingestion pipeline, adds the relevant tasks,
        and returns the configured pipeline.
    """

    def __init__(
        self, config: ServiceConfig, pipeline_cls: type[Pipeline] = DQAPipeline
    ) -> None:
        """
        Initializes the DQAPipelineBuilder with the provided configuration and context.

        Parameters:
        -----------
        config : ServiceConfig
            Configuration object that contains settings and parameters for building the pipeline.
        context : Context
            Context object that tracks metadata related to the pipeline's execution.
        """
        super().__init__(config=config, pipeline_cls=pipeline_cls)

    def create_pipeline(self) -> DQAPipeline:
        """
        Creates and configures the ingestion pipeline with the necessary tasks.

        This method sets up the pipeline by adding tasks such as reading from the source,
        ingesting data, and writing to the target. It returns a fully constructed
        pipeline ready for execution.

        Returns:
        --------
        DQAPipeline:
            The fully configured data ingestion pipeline with all tasks.
        """
        # Instantiate pipeline
        pipe = self._pipeline_cls(config=self._config)

        # Add tasks to pipeline, starting with the lower resource intensive tasks.
        pipe.add_task(self._config.t01_dup1)
        pipe.add_task(self._config.t02_dup2)
        pipe.add_task(self._config.t03_null)
        pipe.add_task(self._config.t04_outlier)
        pipe.add_task(self._config.t05_outlier)
        pipe.add_task(self._config.t06_non_english)
        pipe.add_task(self._config.t07_non_english)
        pipe.add_task(self._config.t08_emoji)
        pipe.add_task(self._config.t09_chars)
        pipe.add_task(self._config.t10_dates)
        pipe.add_task(self._config.t11_ratings)
        pipe.add_task(self._config.t12_profanity)
        pipe.add_task(self._config.t13_emails)
        pipe.add_task(self._config.t14_urls)
        pipe.add_task(self._config.t15_phones)
        return pipe

        return pipe
