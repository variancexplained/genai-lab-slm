#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/metrics/category_author/task.py                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 07:21:26 pm                                                   #
# Modified   : Monday September 16th 2024 01:47:20 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Pre Compute Module"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Union

import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sparkFunc

from discover.application.pipeline import Pipeline, PipelineBuilder
from discover.data_prep.io import ConvertTask, Reader, WriteTask
from discover.domain.base.task import Task
from discover.shared.instrumentation.decorator import profiler
from discover.utils.base import Reader, Writer
from discover.utils.convert import ToPandas, ToSpark
from discover.utils.io import PandasReader, PandasWriter
from discover.utils.repo import ReviewRepo
from discover.utils.tempfile import TempFileMgr

# ------------------------------------------------------------------------------------------------ #
load_dotenv()


# ------------------------------------------------------------------------------------------------ #
#                                        CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class MetricsConfig(ServiceConfig):
    """Base class for configuration"""

    source_directory: str = "04_features/reviews"
    source_filename: str = None
    target_directory: str = "06_metrics"
    force: bool = False


@dataclass
class CategoryMetricsConfig(MetricsConfig):
    name: str = "CategoryMetrics"
    target_filename: str = "category_metrics.pkl"


@dataclass
class AuthorMetricsConfig(MetricsConfig):
    name: str = "AuthorMetrics"
    target_filename: str = "author_metrics.pkl"


@dataclass
class AppMetricsConfig(MetricsConfig):
    name: str = "AppMetrics"
    target_filename: str = "app_metrics.pkl"


@dataclass
class CategoryAuthorMetricsConfig(MetricsConfig):
    name: str = "CategoryAuthorMetrics"
    target_filename: str = "category_author_metrics.pkl"


# ------------------------------------------------------------------------------------------------ #
#                                          METRICS                                                 #
# ------------------------------------------------------------------------------------------------ #
class Metrics(PipelineBuilder):
    """Encapsulates the pre-compute pipeline

    Attributes:
        data (pd.DataFrame): The pre-computed dataset

    Args:
        config (MetricsConfig): Configuration for metrics precomputation.
        spark (SparkSession): Spark session.
        metrics_task_cls (type[MetricsTask]): Precomputation metrics Task class
        pipeline_cls type[Pipeline]: Pipeline class to instantiate
        review_repo_cls (type[ReviewRepo]): Manages dataset IO
        source_reader_cls (type[Reader]): Class for reading the source data.
        target_writer_cls (type[Writer]): Class for writing the target data
        target_reader_cls (type[Reader]): Class for reading the target data.
        tempfile_manager (type[TempFileMgr]): Manages temporary files for pipelines
        to_pandas_cls: (type[toPandas]): Converts Spark DataFrames to Pandas
        to_spark_cls: (type[toSpark]): Converts Pandas DataFrames to Spark

    """

    def __init__(
        self,
        config: MetricsConfig,
        spark: SparkSession,
        metrics_task_cls: type[MetricsTask],
        source_reader_cls: type[Reader] = PandasReader,
        target_writer_cls: type[Writer] = PandasWriter,
        target_reader_cls: type[Reader] = PandasReader,
        pipeline_cls: type[Pipeline] = Pipeline,
        review_repo_cls: type[ReviewRepo] = ReviewRepo,
        tempfile_manager_cls: type[TempFileMgr] = TempFileMgr,
        to_pandas_cls: type[ToPandas] = ToPandas,
        to_spark_cls: type[ToSpark] = ToSpark,
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
        self._metrics_task_cls = metrics_task_cls
        self._to_pandas_cls = to_pandas_cls
        self._to_spark_cls = to_spark_cls
        self._spark = spark
        self._tempfile_manager_cls = tempfile_manager_cls

    def create_pipeline(self, tempfile_manager: TempFileMgr) -> Pipeline:
        """Creates the pipeline with all the tasks for data quality analysis.

        Args:
            tempfile_manager (TempFileMgr): Manages temp files in context.

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

        # Convert the pandas DataFrame to a Spark DataFrame
        to_spark = ConvertTask(
            converter_cls=self._to_spark_cls,
            spark=self._spark,
            task_cls=self._metrics_task_cls,
        )

        # Metric precomputation instance.
        metrics = self._metrics_task_cls()

        # Convert Spark DataFrame back to Pandas.
        to_pandas = ConvertTask(
            converter_cls=self._to_pandas_cls, task_cls=self._metrics_task_cls
        )

        # Persist the data
        save = WriteTask(
            directory=self.config.target_directory,
            filename=self.config.target_filename,
            writer_cls=self.target_writer_cls,
        )

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(to_spark)
        pipe.add_task(metrics)
        pipe.add_task(to_pandas)
        pipe.add_task(save)
        return pipe

    def run(self) -> Union[pd.DataFrame, DataFrame]:
        """Executes the preprocessing tasks.

        The pipeline runs if the endpoint doesn't already exist or if
        the config.force is True. If the endpoint already exists and the
        config.force is False, the endpoint is read and returned.
        """
        if self.endpoint_exists() and not self.config.force:
            self._data = self.read_endpoint()
        else:
            with self._tempfile_manager_cls() as tempfile_manager:
                pipeline = self.create_pipeline(tempfile_manager)
                self._data = pipeline.run()
        return self._data


# ------------------------------------------------------------------------------------------------ #
#                                         METRICS                                                  #
# ------------------------------------------------------------------------------------------------ #
class MetricsTask(Task):
    """Abstract base class for MetricsTasks"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)


# ------------------------------------------------------------------------------------------------ #
#                                     CATEGORY METRICS                                             #
# ------------------------------------------------------------------------------------------------ #
class CategoryMetricsTask(MetricsTask):
    """Computes and saves metrics aggregated to the category level."""

    def __init__(self) -> None:
        super().__init__()

    @announcer
    @profiler
    def run(self, data: DataFrame) -> DataFrame:
        """Aggregates and creates metrics at the category level.

        Args:
            data (DataFrame): PySpark DataFrame containing app reviews.
        """

        df = data.groupBy("category").agg(
            sparkFunc.countDistinct(sparkFunc.col("app_id")).alias("app_count"),
            sparkFunc.countDistinct(sparkFunc.col("author")).alias("author_count"),
            sparkFunc.count("*").alias("review_count"),
            sparkFunc.min(sparkFunc.col("rating")).alias("rating_min"),
            sparkFunc.max(sparkFunc.col("rating")).alias("rating_max"),
            sparkFunc.avg(sparkFunc.col("rating")).alias("rating_avg"),
            sparkFunc.mode(sparkFunc.col("rating")).alias("rating_mode"),
            sparkFunc.stddev(sparkFunc.col("rating")).alias("rating_std"),
            sparkFunc.min(sparkFunc.col("review_length")).alias("review_length_min"),
            sparkFunc.max(sparkFunc.col("review_length")).alias("review_length_max"),
            sparkFunc.avg(sparkFunc.col("review_length")).alias("review_length_avg"),
            sparkFunc.mode(sparkFunc.col("review_length")).alias("review_length_mode"),
            sparkFunc.stddev(sparkFunc.col("review_length")).alias("review_length_std"),
            sparkFunc.min(sparkFunc.col("vote_count")).alias("vote_count_min"),
            sparkFunc.max(sparkFunc.col("vote_count")).alias("vote_count_max"),
            sparkFunc.avg(sparkFunc.col("vote_count")).alias("vote_count_avg"),
            sparkFunc.mode(sparkFunc.col("vote_count")).alias("vote_count_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_count")).alias("vote_count_std"),
            sparkFunc.min(sparkFunc.col("vote_sum")).alias("vote_sum_min"),
            sparkFunc.max(sparkFunc.col("vote_sum")).alias("vote_sum_max"),
            sparkFunc.avg(sparkFunc.col("vote_sum")).alias("vote_sum_avg"),
            sparkFunc.mode(sparkFunc.col("vote_sum")).alias("vote_sum_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_sum")).alias("vote_sum_std"),
            sparkFunc.min(sparkFunc.col("date")).alias("date_min"),
            sparkFunc.max(sparkFunc.col("date")).alias("date_max"),
            sparkFunc.mode(sparkFunc.col("date")).alias("date_mode"),
        )

        df2 = df.withColumn(
            "reviews_per_app", df["review_count"] / df["app_count"]
        ).withColumn("reviews_per_author", df["review_count"] / df["author_count"])

        results = df2.select(
            "category",
            "app_count",
            "author_count",
            "review_count",
            "reviews_per_app",
            "reviews_per_author",
            "rating_min",
            "rating_max",
            "rating_avg",
            "rating_mode",
            "rating_std",
            "review_length_min",
            "review_length_max",
            "review_length_avg",
            "review_length_mode",
            "review_length_std",
            "vote_count_min",
            "vote_count_max",
            "vote_count_avg",
            "vote_count_mode",
            "vote_count_std",
            "vote_sum_min",
            "vote_sum_max",
            "vote_sum_avg",
            "vote_sum_mode",
            "vote_sum_std",
            "date_min",
            "date_max",
            "date_mode",
        )
        return results


# ------------------------------------------------------------------------------------------------ #
#                                      AUTHOR METRICS                                              #
# ------------------------------------------------------------------------------------------------ #
class AuthorMetricsTask(MetricsTask):
    """Computes and saves metrics aggregated to the author level."""

    def __init__(self) -> None:
        super().__init__()

    @announcer
    @profiler
    def run(self, data: DataFrame) -> DataFrame:
        """Aggregates and creates metrics at the author level.

        Args:
            data (DataFrame): PySpark DataFrame containing app reviews.
        """

        df = data.groupBy("author").agg(
            sparkFunc.countDistinct(sparkFunc.col("app_id")).alias("app_count"),
            sparkFunc.countDistinct(sparkFunc.col("category")).alias("category"),
            sparkFunc.count("*").alias("review_count"),
            sparkFunc.min(sparkFunc.col("rating")).alias("rating_min"),
            sparkFunc.max(sparkFunc.col("rating")).alias("rating_max"),
            sparkFunc.avg(sparkFunc.col("rating")).alias("rating_avg"),
            sparkFunc.mode(sparkFunc.col("rating")).alias("rating_mode"),
            sparkFunc.stddev(sparkFunc.col("rating")).alias("rating_std"),
            sparkFunc.min(sparkFunc.col("review_length")).alias("review_length_min"),
            sparkFunc.max(sparkFunc.col("review_length")).alias("review_length_max"),
            sparkFunc.avg(sparkFunc.col("review_length")).alias("review_length_avg"),
            sparkFunc.mode(sparkFunc.col("review_length")).alias("review_length_mode"),
            sparkFunc.stddev(sparkFunc.col("review_length")).alias("review_length_std"),
            sparkFunc.min(sparkFunc.col("vote_count")).alias("vote_count_min"),
            sparkFunc.max(sparkFunc.col("vote_count")).alias("vote_count_max"),
            sparkFunc.avg(sparkFunc.col("vote_count")).alias("vote_count_avg"),
            sparkFunc.mode(sparkFunc.col("vote_count")).alias("vote_count_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_count")).alias("vote_count_std"),
            sparkFunc.min(sparkFunc.col("vote_sum")).alias("vote_sum_min"),
            sparkFunc.max(sparkFunc.col("vote_sum")).alias("vote_sum_max"),
            sparkFunc.avg(sparkFunc.col("vote_sum")).alias("vote_sum_avg"),
            sparkFunc.mode(sparkFunc.col("vote_sum")).alias("vote_sum_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_sum")).alias("vote_sum_std"),
            sparkFunc.min(sparkFunc.col("date")).alias("date_min"),
            sparkFunc.max(sparkFunc.col("date")).alias("date_max"),
            sparkFunc.mode(sparkFunc.col("date")).alias("date_mode"),
        )
        df2 = df.withColumn(
            "reviews_per_app", df["review_count"] / df["app_count"]
        ).withColumn("reviews_per_category", df["review_count"] / df["category"])

        results = df2.select(
            "author",
            "category",
            "app_count",
            "review_count",
            "reviews_per_app",
            "reviews_per_category",
            "rating_min",
            "rating_max",
            "rating_avg",
            "rating_mode",
            "rating_std",
            "review_length_min",
            "review_length_max",
            "review_length_avg",
            "review_length_mode",
            "review_length_std",
            "vote_count_min",
            "vote_count_max",
            "vote_count_avg",
            "vote_count_mode",
            "vote_count_std",
            "vote_sum_min",
            "vote_sum_max",
            "vote_sum_avg",
            "vote_sum_mode",
            "vote_sum_std",
            "date_min",
            "date_max",
            "date_mode",
        )
        return results


# ------------------------------------------------------------------------------------------------ #
#                                       APP METRICS                                                #
# ------------------------------------------------------------------------------------------------ #
class AppMetricsTask(MetricsTask):
    """Computes and saves metrics aggregated to the app level."""

    def __init__(self) -> None:
        super().__init__()

    @announcer
    @profiler
    def run(self, data: DataFrame) -> DataFrame:
        """Aggregates and creates metrics at the app level.

        Args:
            data (DataFrame): PySpark DataFrame containing app reviews.
        """

        df = data.groupBy("app_name").agg(
            sparkFunc.countDistinct(sparkFunc.col("author")).alias("author_count"),
            sparkFunc.count("*").alias("review_count"),
            sparkFunc.min(sparkFunc.col("rating")).alias("rating_min"),
            sparkFunc.max(sparkFunc.col("rating")).alias("rating_max"),
            sparkFunc.avg(sparkFunc.col("rating")).alias("rating_avg"),
            sparkFunc.mode(sparkFunc.col("rating")).alias("rating_mode"),
            sparkFunc.stddev(sparkFunc.col("rating")).alias("rating_std"),
            sparkFunc.min(sparkFunc.col("review_length")).alias("review_length_min"),
            sparkFunc.max(sparkFunc.col("review_length")).alias("review_length_max"),
            sparkFunc.avg(sparkFunc.col("review_length")).alias("review_length_avg"),
            sparkFunc.mode(sparkFunc.col("review_length")).alias("review_length_mode"),
            sparkFunc.stddev(sparkFunc.col("review_length")).alias("review_length_std"),
            sparkFunc.min(sparkFunc.col("vote_count")).alias("vote_count_min"),
            sparkFunc.max(sparkFunc.col("vote_count")).alias("vote_count_max"),
            sparkFunc.avg(sparkFunc.col("vote_count")).alias("vote_count_avg"),
            sparkFunc.mode(sparkFunc.col("vote_count")).alias("vote_count_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_count")).alias("vote_count_std"),
            sparkFunc.min(sparkFunc.col("vote_sum")).alias("vote_sum_min"),
            sparkFunc.max(sparkFunc.col("vote_sum")).alias("vote_sum_max"),
            sparkFunc.avg(sparkFunc.col("vote_sum")).alias("vote_sum_avg"),
            sparkFunc.mode(sparkFunc.col("vote_sum")).alias("vote_sum_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_sum")).alias("vote_sum_std"),
            sparkFunc.min(sparkFunc.col("date")).alias("date_min"),
            sparkFunc.max(sparkFunc.col("date")).alias("date_max"),
            sparkFunc.mode(sparkFunc.col("date")).alias("date_mode"),
        )
        df2 = df.withColumn(
            colName="reviews_per_author", col=(df["review_count"] / df["author_count"])
        )

        results = df2.select(
            "app_name",
            "author_count",
            "review_count",
            "reviews_per_author",
            "rating_min",
            "rating_max",
            "rating_avg",
            "rating_mode",
            "rating_std",
            "review_length_min",
            "review_length_max",
            "review_length_avg",
            "review_length_mode",
            "review_length_std",
            "vote_count_min",
            "vote_count_max",
            "vote_count_avg",
            "vote_count_mode",
            "vote_count_std",
            "vote_sum_min",
            "vote_sum_max",
            "vote_sum_avg",
            "vote_sum_mode",
            "vote_sum_std",
            "date_min",
            "date_max",
            "date_mode",
        )
        return results


# ------------------------------------------------------------------------------------------------ #
#                                 CATEGORY / AUTHOR METRICS                                        #
# ------------------------------------------------------------------------------------------------ #
class CategoryAuthorMetricsTask(MetricsTask):
    """Computes and saves metrics aggregated to the category/author level."""

    def __init__(self) -> None:
        super().__init__()

    @announcer
    @profiler
    def run(self, data: DataFrame) -> DataFrame:
        """Aggregates and creates metrics at the category/author level.

        Args:
            data (DataFrame): PySpark DataFrame containing app reviews.
        """

        df = data.groupBy("category", "author").agg(
            sparkFunc.countDistinct(sparkFunc.col("app_id")).alias("app_count"),
            sparkFunc.count("*").alias("review_count"),
            sparkFunc.min(sparkFunc.col("rating")).alias("rating_min"),
            sparkFunc.max(sparkFunc.col("rating")).alias("rating_max"),
            sparkFunc.avg(sparkFunc.col("rating")).alias("rating_avg"),
            sparkFunc.mode(sparkFunc.col("rating")).alias("rating_mode"),
            sparkFunc.stddev(sparkFunc.col("rating")).alias("rating_std"),
            sparkFunc.min(sparkFunc.col("review_length")).alias("review_length_min"),
            sparkFunc.max(sparkFunc.col("review_length")).alias("review_length_max"),
            sparkFunc.avg(sparkFunc.col("review_length")).alias("review_length_avg"),
            sparkFunc.mode(sparkFunc.col("review_length")).alias("review_length_mode"),
            sparkFunc.stddev(sparkFunc.col("review_length")).alias("review_length_std"),
            sparkFunc.min(sparkFunc.col("vote_count")).alias("vote_count_min"),
            sparkFunc.max(sparkFunc.col("vote_count")).alias("vote_count_max"),
            sparkFunc.avg(sparkFunc.col("vote_count")).alias("vote_count_avg"),
            sparkFunc.mode(sparkFunc.col("vote_count")).alias("vote_count_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_count")).alias("vote_count_std"),
            sparkFunc.min(sparkFunc.col("vote_sum")).alias("vote_sum_min"),
            sparkFunc.max(sparkFunc.col("vote_sum")).alias("vote_sum_max"),
            sparkFunc.avg(sparkFunc.col("vote_sum")).alias("vote_sum_avg"),
            sparkFunc.mode(sparkFunc.col("vote_sum")).alias("vote_sum_mode"),
            sparkFunc.stddev(sparkFunc.col("vote_sum")).alias("vote_sum_std"),
            sparkFunc.min(sparkFunc.col("date")).alias("date_min"),
            sparkFunc.max(sparkFunc.col("date")).alias("date_max"),
            sparkFunc.mode(sparkFunc.col("date")).alias("date_mode"),
        )

        df2 = df.withColumn("reviews_per_app", df["review_count"] / df["app_count"])

        results = df2.select(
            "category",
            "author",
            "app_count",
            "review_count",
            "reviews_per_app",
            "rating_min",
            "rating_max",
            "rating_avg",
            "rating_mode",
            "rating_std",
            "review_length_min",
            "review_length_max",
            "review_length_avg",
            "review_length_mode",
            "review_length_std",
            "vote_count_min",
            "vote_count_max",
            "vote_count_avg",
            "vote_count_mode",
            "vote_count_std",
            "vote_sum_min",
            "vote_sum_max",
            "vote_sum_avg",
            "vote_sum_mode",
            "vote_sum_std",
            "date_min",
            "date_max",
            "date_mode",
        )
        return results
