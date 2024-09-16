#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/data_cleaning/config.py                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday May 29th 2024 10:08:16 am                                                 #
# Modified   : Monday September 16th 2024 01:47:18 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Clean Data Module"""
import os
from dataclasses import dataclass, field

import pandas as pd
from dotenv import load_dotenv

from discover.application.pipeline import Pipeline, PipelineBuilder, ServiceConfig
from discover.data_prep.io import Reader, Writer
from discover.domain.base.task import Task
from discover.shared.instrumentation.decorator import profiler
from discover.shared.persist.file.io import IOService
from discover.utils.base import Reader, Writer
from discover.utils.io import PandasReader, PandasWriter
from discover.utils.print import Printer
from discover.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
#                                    CLEAN CONFIG                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class CleanConfig(ServiceConfig):
    """Class encapsulating the configuration for the data cleaning stage."""

    name: str = "DataCleaner"
    source_directory: str = "02_dqa/reviews"
    source_filename: str = None
    target_directory: str = "03_clean/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    force: bool = False
    impact_to_remove: list = field(default_factory=lambda: ["Critical", "High"])
    config: pd.DataFrame = None

    def __post_init__(self) -> None:
        filepath = os.getenv("CONFIG_CLEAN")
        if filepath is None:
            msg = "No clean configuration filepath declared in .env"
            raise RuntimeError(msg)

        self.config = IOService.read(filepath=filepath)

    def get_issues_to_remove(self) -> list:
        return list(
            self.config.loc[self.config["Impact"].isin(self.impact_to_remove), "Issue"]
        )


# ------------------------------------------------------------------------------------------------ #
#                                        CLEAN                                                     #
# ------------------------------------------------------------------------------------------------ #
class DataCleaner(PipelineBuilder):
    """Encapsulates the data cleaning pipeline

    Attributes:
        data (pd.DataFrame): The cleaned dataset

    Args:
        config (ServiceConfig): Configuration for the subclass stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.

    """

    def __init__(
        self,
        config: ServiceConfig,
        source_reader_cls: type[Reader] = PandasReader,
        target_writer_cls: type[Writer] = PandasWriter,
        target_reader_cls: type[Reader] = PandasReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
    ) -> None:
        """Initializes the DataQualityPipeline with data."""
        super().__init__(
            config=config,
            source_reader_cls=source_reader_cls,
            target_writer_cls=target_writer_cls,
            target_reader_cls=target_reader_cls,
            pipeline_cls=pipeline_cls,
            review_repo_cls=review_repo_cls,
        )
        self._overview = None

    def overview(self) -> pd.DataFrame:
        if self._overview is None:
            self._overview = (
                self._data.loc[:, self._data.columns.str.startswith("dqa_")]
                .sum()
                .to_frame()
            )
            self._overview.columns = ["Count"]
            self._overview["Percent"] = self._overview["Count"] / len(self._data) * 100
        return self._overview

    def create_pipeline(self) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = self._pipeline_cls(name=self.config.name)

        # Instantiate Tasks
        load = Reader(
            directory=self.config.source_directory,
            filename=self.config.source_filename,
            reader_cls=self.source_reader_cls,
        )
        save = Writer(
            directory=self.config.target_directory,
            filename=self.config.target_filename,
            writer_cls=self.target_writer_cls,
            partition_cols=self.config.partition_cols,
        )
        clean = DataCleaningTask(issues_to_remove=self.config.get_issues_to_remove())

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(clean)
        pipe.add_task(save)
        return pipe


# ------------------------------------------------------------------------------------------------ #
#                                        CLEAN                                                     #
# ------------------------------------------------------------------------------------------------ #
class DataCleaningTask(Task):
    """A Task class for cleaning a DataFrame based on a list of issues to remove.

    This class executes data cleaning operations on a DataFrame, removing rows
    containing specified issues based on configuration rules.

    """

    def __init__(self, issues_to_remove: list) -> None:
        """Initializes the DataCleaningTask with an IOService instance.

        Args:
            issues_to_remove (list): List of issues to remove from dataset.
        """
        super().__init__()
        self._issues_to_remove = issues_to_remove
        self._printer = Printer()

    @announcer
    @profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task, cleaning the DataFrame based on configuration rules.

        Args:
            data (pd.DataFrame): Review data.

        Returns:
            pd.DataFrame: Cleaned DataFrame with specified issues removed.
        """

        try:
            mask_to_remove = data[self._issues_to_remove].sum(axis=1) > 0
            clean_df = data[~mask_to_remove]
            dirty_df = data[mask_to_remove]
        except KeyError as e:
            raise KeyError(f"Column not found in the DataFrame: {e}")
        except Exception as e:
            raise RuntimeError(f"Error during data cleaning: {e}")

        # Remove dqa columns
        cols_to_drop = clean_df.columns[clean_df.columns.str.contains("dqa")]
        clean_df = clean_df.drop(cols_to_drop, axis=1)
        clean_df = clean_df.sort_values(by="date")

        title = "AppVoCAI-Discover Data Cleaning"
        d = {
            "Original DataFrame": f"{data.shape[0]} rows",
            "Cleaned DataFrame": f"{clean_df.shape[0]} rows",
            "Removed Observations": f"{dirty_df.shape[0]} rows",
        }

        self._printer.print_dict(title=title, data=d)

        return clean_df
