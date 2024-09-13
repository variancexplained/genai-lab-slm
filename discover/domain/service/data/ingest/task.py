#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data/ingest/task.py                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday May 24th 2024 02:47:03 am                                                    #
# Modified   : Friday September 13th 2024 02:34:29 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Stage Module"""
from dataclasses import dataclass, field
from typing import Dict

import pandas as pd
from dotenv import load_dotenv
from pandarallel import pandarallel

from discover.application.data_prep.io import ReadTask, WriteTask
from discover.application.pipeline import Pipeline, PipelineBuilder, StageConfig, Task
from discover.shared.instrumentation.decorator import task_profiler
from discover.shared.logging.logging import log_exceptions
from discover.utils.base import Reader, Writer
from discover.utils.cast import CastPandas
from discover.utils.io import PandasReader, PandasWriter
from discover.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
load_dotenv()
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


@dataclass
class StageConfig(StageConfig):
    """Data processing configuration for the Stage"""

    name: str = "Stage"
    source_directory: str = "00_raw/reviews"
    source_filename: str = None
    target_directory: str = "01_stage/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    text_column: str = "content"
    force: bool = False
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


# ------------------------------------------------------------------------------------------------ #
#                                        NORMALIZE                                                 #
# ------------------------------------------------------------------------------------------------ #
class Stage(PipelineBuilder):
    """Encapsulates the data normalization pipeline

    Attributes:
        data (pd.DataFrame): The normalized dataset

    Args:
        config (StageConfig): Configuration for the subclass stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.

    """

    def __init__(
        self,
        config: StageConfig,
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

    def create_pipeline(self) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Returns:
            Pipeline: The configured pipeline with tasks.
        """
        # Instantiate pipeline
        pipe = self.pipeline_cls(name=self.config.name)

        # Instantiate Tasks
        load = ReadTask(
            directory=self.config.source_directory,
            filename=self.config.source_filename,
            reader_cls=self.source_reader_cls,
        )
        save = WriteTask(
            directory=self.config.target_directory,
            filename=self.config.target_filename,
            writer_cls=self.target_writer_cls,
            partition_cols=self.config.partition_cols,
        )
        normalize = StageDataTask(
            datatypes=self.config.datatypes,
            text_column=self.config.text_column,
            cast_cls=CastPandas,
            encoding_sample=self.config.encoding_sample,
            random_state=self.config.random_state,
        )

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(normalize)
        pipe.add_task(save)
        return pipe


# ------------------------------------------------------------------------------------------------ #
class StageDataTask(Task):
    def __init__(
        self,
        datatypes: dict,
        text_column: str = "content",
        cast_cls: type[CastPandas] = CastPandas,
        encoding_sample: float = 0.01,
        random_state: int = None,
    ):
        """
        Initializes the StageDataTask.

        The purpose of this class is to perform the minimum necessary preconditioning
        of the data, keeping it as close to its original form as possible, while ensuring
        that downstream data quality, processing, and analysis code can execute.

        Args:
            datatypes (dict): Mapping of columns to data types.
            text_column (str): Name of the column containing text data to be preprocessed.
            cast (CastPandas): Object to cast the columns to a designated data type

        """
        super().__init__()
        self._datatypes = datatypes
        self._text_column = text_column
        self._cast_cls = cast_cls
        self._cast = cast_cls()
        self._encoding_sample = encoding_sample
        self._random_state = random_state

    @task_profiler
    @log_exceptions()
    def run(self, data: pd.DataFrame):
        """Preprocess text data by ensuring string column, removing newlines, and verifying encoding."""

        data = self._remove_newlines(data=data)
        data = self._verify_encoding(data=data)
        data = self._cast_datatypes(data=data, datatypes=self._datatypes)
        data = self._trim_dataset(data=data)

        return data

    def _remove_newlines(self, data):
        """Remove newline characters from the specified column."""
        data[self._text_column] = data[self._text_column].str.replace("\n", " ")
        self._logger.debug("Removed newlines")
        return data

    def _verify_encoding(self, data):
        """Verify and normalize the encoding of the specified column.

        A sample is checked for encoding errors. If encoding errors encountered,
        the entire data column is encoded. Otherwise, the encoding is skipped.
        """

        def check_sample_encoding(sample) -> bool:
            try:
                sample.parallel_apply(lambda x: x.encode("utf-8").decode("utf-8"))
                return False  # No encoding issues found
            except UnicodeEncodeError:
                return True  # Encoding issues found

        def re_encode_text(text):
            """Re-encode text to handle encoding issues."""
            try:
                return text.encode("utf-8").decode("utf-8")
            except UnicodeEncodeError:
                self._logger.debug(f"Encoding issue found in text: {text}")
                return text.encode("utf-8", errors="ignore").decode("utf-8")

        sample = data[self._text_column].sample(
            frac=self._encoding_sample, random_state=self._random_state
        )
        if check_sample_encoding(sample=sample):
            self._logger.debug(
                "Encoding issues found in sample. Re-encoding the entire column."
            )
            data[self._text_column] = data[self._text_column].parallel_apply(
                self._re_encode_text
            )
        else:
            self._logger.debug(
                "No encoding issues found in sample. Skipping re-encoding."
            )
        return data

    def _cast_datatypes(
        self, data: pd.DataFrame, datatypes: Dict[str, type]
    ) -> pd.DataFrame:
        """Casts columns to the designated data types"""
        data = self._cast.apply(data=data, datatypes=datatypes)
        self._logger.debug("Cast data types")
        return data

    def _trim_dataset(self, data: pd.DataFrame) -> None:
        """Drop shopping review"""
        # We only have about 9 reviews in this category.
        data = data.loc[data["category"] != "Shopping"]
        data["category"] = data["category"].cat.remove_unused_categories()
        self._logger.debug("Trimmed dataset of unused categories.")
        return data
