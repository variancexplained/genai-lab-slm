#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/application/service/data/dqa/config.py                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 18th 2024 12:24:56 am                                           #
# Modified   : Thursday September 19th 2024 01:11:54 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  DQAion Application Service Config"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from discover.domain.base.repo import Repo
from discover.domain.entity.config import DataConfig, ServiceConfig
from discover.domain.task.data.dqa import (
    DetectDuplicateRowTask,
    DetectEmailTask,
    DetectEmojiTask,
    DetectInvalidDatesTask,
    DetectInvalidRatingsTask,
    DetectNonEnglishTask,
    DetectNullValuesTask,
    DetectOutliersTask,
    DetectPhoneNumberTask,
    DetectProfanityTask,
    DetectSpecialCharacterTask,
    DetectURLTask,
)
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage
from discover.infra.repo.factory import ReviewRepoFactory

# ------------------------------------------------------------------------------------------------ #
repo_factory = ReviewRepoFactory()


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQASourceDataConfig(DataConfig):
    """
    Configuration class for source data in the Data Quality Assessment (DQA) phase.

    This class extends `DataConfig` and provides specific default values for the DQA process,
    particularly for the data ingestion stage. It is designed to configure the source data
    during the Data Quality Assessment phase of the data preparation pipeline.

    Attributes:
    -----------
    phase : Phase, default=Phase.DATAPREP
        The lifecycle phase of the data pipeline, set to `DATAPREP` by default, indicating
        that this configuration is used during the data preparation phase.

    stage : Stage, default=DataPrepStage.INGEST
        The stage of the data preparation process, set to `INGEST`, specifying that the data
        being processed is in the ingestion stage of the pipeline.

    name : str, default="reviews"
        The name of the dataset being processed, set to "reviews" by default, representing
        the data source being used for the DQA process.

    Inherits:
    ---------
    DataConfig:
        Inherits all attributes and methods from the `DataConfig` class, which manages configuration
        for data structures, file formats, partitioning, and other dataset-specific details.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQATargetDataConfig(DataConfig):
    """
    Configuration class for target data in the Data Quality Assessment (DQA) phase.

    This class extends `DataConfig` and provides default values for the DQA process,
    specifically for configuring the target data during the DQA stage. It is designed
    to manage the target dataset settings used during the Data Quality Assessment
    phase of the data preparation pipeline.

    Attributes:
    -----------
    phase : Phase, default=Phase.DATAPREP
        The lifecycle phase of the data pipeline, set to `DATAPREP`, indicating that this
        configuration is used during the data preparation phase.

    stage : Stage, default=DataPrepStage.DQA
        The stage of the data preparation process, set to `DQA`, representing the Data Quality
        Assessment stage of the pipeline.

    name : str, default="reviews"
        The name of the target dataset being processed, set to "reviews" by default, representing
        the dataset to which the DQA process will be applied.

    Inherits:
    ---------
    DataConfig:
        Inherits all attributes and methods from the `DataConfig` class, which manages configuration
        for data structures, file formats, partitioning, and other dataset-specific details.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.DQA
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQAServiceConfig(ServiceConfig):
    """
    Configuration class for the Data Quality Assessment (DQA) service.

    This class extends `ServiceConfig` and provides configurations specific to the DQA process.
    It includes task definitions, data configurations for source and target datasets,
    concurrency settings, and various task-specific parameters. This configuration is designed
    to initialize and execute tasks for assessing the quality of the data during the DQA stage.

    Attributes:
    -----------
    repo : Optional[Repo], default=None
        The repository object used to access data. It is initialized in `__post_init__`
        based on the `source_data_config`.

    source_data_config : DataConfig, default=DQASourceDataConfig()
        Configuration object for the source data, detailing the source dataset for the DQA process.

    target_data_config : DataConfig, default=DQATargetDataConfig()
        Configuration object for the target data, defining how the data should be written or handled
        after processing.

    phase : Phase, default=Phase.DATAPREP
        The lifecycle phase for the DQA service, set to `DATAPREP` to indicate that the
        DQA process is part of the data preparation phase.

    stage : Stage, default=DataPrepStage.DQA
        The specific stage within the data preparation process, set to `DQA` to denote that
        this configuration applies to the Data Quality Assessment stage.

    n_jobs : int, default=12
        The number of jobs or threads to use for tasks that support parallelism.

    init_sort_by : str, default="id"
        The column used for sorting during initialization of the DQA process, defaulting to "id".

    Tasks:
    ------
    t01_dup1 : DetectDuplicateRowTask
        Task to detect duplicate rows in the dataset.

    to2_dup2 : DetectDuplicateRowTask
        Task to detect duplicate review IDs.

    to3_null : DetectNullValuesTask
        Task to detect null values in the dataset.

    t04_outlier : DetectOutliersTask
        Task to detect outliers based on the "vote_sum" column.

    t05_outlier : DetectOutliersTask
        Task to detect outliers based on the "vote_count" column.

    t06_non_english : DetectNonEnglishTask
        Task to detect non-English content in the "content" column.

    t07_non_english : DetectNonEnglishTask
        Task to detect non-English content in the "app_name" column.

    t08_emoji : DetectEmojiTask
        Task to detect emojis in the dataset.

    t09_chars : DetectSpecialCharacterTask
        Task to detect excessive special characters in text data, with a threshold of 0.3.

    t10_dates : DetectInvalidDatesTask
        Task to detect invalid dates in the dataset.

    t11_ratings : DetectInvalidRatingsTask
        Task to detect invalid ratings in the dataset.

    t12_profanity : DetectProfanityTask
        Task to detect profanity in text columns, using 12 parallel jobs.

    t13_emails : DetectEmailTask
        Task to detect email addresses in the dataset.

    t14_urls : DetectURLTask
        Task to detect URLs in the dataset.

    t15_phones : DetectPhoneNumberTask
        Task to detect phone numbers in the dataset.

    force : bool, default=False
        A flag indicating whether to force the execution of tasks, potentially overriding caching
        or other checks.

    Methods:
    --------
    __post_init__() -> None:
        Initializes the repository by calling the `repo_factory` to get a repository instance
        based on the `source_data_config`.
    """

    repo: Optional[Repo] = None
    # Data configuration
    source_data_config: DataConfig = DQASourceDataConfig()
    target_data_config: DataConfig = DQATargetDataConfig()
    # Stage config
    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.DQA
    # Concurrency parameter
    n_jobs: int = 12
    # Sort column in initialization
    init_sort_by: str = "id"
    # -------------------------------------------------------------------------------------------- #
    # Duplicate row checker
    t01_dup1 = DetectDuplicateRowTask(new_column_name="is_duplicate")
    # duplicate review id checker
    to2_dup2 = DetectDuplicateRowTask(
        column_names="id", new_column_name="is_duplicate_review_id"
    )
    # Detection of null values
    to3_null = DetectNullValuesTask(new_column_name="has_null_values")
    # -------------------------------------------------------------------------------------------- #
    # Outlier detectors
    t04_outlier = DetectOutliersTask(
        column_name="vote_sum", new_column_name="vote_sum_outlier"
    )
    t05_outlier = DetectOutliersTask(
        column_name="vote_count", new_column_name="vote_count_outlier"
    )
    # Detect Non-English in Reviews and app name
    t06_non_english = DetectNonEnglishTask(
        text_column="content", new_column_name="has_non_english_review", n_jobs=12
    )
    t07_non_english = DetectNonEnglishTask(
        text_column="app_name", new_column_name="has_non_english_app_name", n_jobs=12
    )
    # Emoji Detection
    t08_emoji = DetectEmojiTask(new_column_name="has_emojis")
    # Excessive Special Character Detection
    t09_chars = DetectSpecialCharacterTask(
        threshold=0.3, new_column_name="has_excessive_special_chars"
    )
    # Date checker
    t10_dates = DetectInvalidDatesTask(new_column_name="has_invalid_date")
    # Rating Checker
    t11_ratings = DetectInvalidRatingsTask(new_column_name="has_invalid_rating")
    # Profanity Check
    t12_profanity = DetectProfanityTask(n_jobs=12, new_column_name="has_profanity")
    # Email, URL, and phone number detection
    t13_emails = DetectEmailTask(new_column_name="contains_email")
    t14_urls = DetectURLTask(new_column_name="contains_url")
    t15_phones = DetectPhoneNumberTask(new_column_name="contains_phone_number")
    # Force
    force: bool = False

    def __post_init__(self) -> None:
        self.repo = repo_factory.get_repo(config=self.source_data_config)
