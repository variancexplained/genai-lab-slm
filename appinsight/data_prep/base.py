#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/data_prep/base.py                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 1st 2024 07:16:33 pm                                                  #
# Modified   : Friday June 28th 2024 06:50:19 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
from abc import ABC, abstractmethod
from typing import Union
import pandas as pd
from pyspark.sql import DataFrame

from appinsight.utils.base import Reader, Writer
from appinsight.utils.io import FileReader, FileWriter
from appinsight.utils.repo import DatasetRepo
from appinsight.workflow.config import StageConfig
from appinsight.workflow.pipeline import Pipeline


# ------------------------------------------------------------------------------------------------ #
#                                      PREPROCESSOR                                                #
# ------------------------------------------------------------------------------------------------ #
class Preprocessor(ABC):
    """Abstract base class for data preprocessor classes

    Args:
        config (StageConfig): Configuration for the subclass stage.
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        dsm_cls (type[DatasetRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
    """

    def __init__(
        self,
        config: StageConfig,
        source_reader_cls: type[Reader] = FileReader,
        target_writer_cls: type[Writer] = FileWriter,
        target_reader_cls: type[Reader] = FileReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        dsm_cls: type[DatasetRepo] = DatasetRepo,
        **kwargs,
    ) -> None:
        super().__init__()
        self.config = config
        self.source_reader_cls = source_reader_cls
        self.target_writer_cls = target_writer_cls
        self.target_reader_cls = target_reader_cls
        self.pipeline_cls = pipeline_cls
        self.dsm = dsm_cls()
        self._data = None
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def data(self) -> Union[pd.DataFrame, DataFrame]:
        return self._data

    @abstractmethod
    def create_pipeline(self) -> Union[pd.DataFrame, DataFrame]:
        """Constructs the pipeline that executes the preprocessing tasks."""

    def execute(self) -> Union[pd.DataFrame, DataFrame]:
        """Executes the preprocessing tasks.

        The pipeline runs if the endpoint doesn't already exist or if
        the config.force is True. If the endpoint already exists and the
        config.force is False, the endpoint is read and returned.
        """
        self.logger.debug("Checking if endpoint exists.")
        if self.endpoint_exists() and not self.config.force:
            self.logger.info("Endpoint exists. Returning data from endpoint.")
            self._data = self.read_endpoint()
        else:
            self.logger.debug("Creating pipeline")
            pipeline = self.create_pipeline()
            self.logger.debug("Pipeline created.")
            self._data = pipeline.execute()
        return self._data

    def endpoint_exists(self) -> bool:
        """Returns True if the target already exists. False otherwise"""

        try:
            return self.dsm.exists(
                directory=self.config.target_directory,
                filename=self.config.target_filename,
            )
        except FileNotFoundError:
            return False

    def read_endpoint(self) -> Union[pd.DataFrame, DataFrame]:
        """Reads and returns the target data."""
        filepath = self.dsm.get_filepath(
            directory=self.config.target_directory, filename=self.config.target_filename
        )
        try:
            data = self.target_reader_cls().read(filepath=filepath)
            msg = (
                f"{self.config.name} endpoint already exists. Returning prior results."
            )
            self.logger.debug(msg)
            return data
        except Exception as e:
            msg = f"Exception occurred while reading endpoint at {filepath}.\n{e}"
            self.logger.exception(msg)
            raise
