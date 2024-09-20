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
# Modified   : Friday September 20th 2024 01:03:55 am                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data  DQAion Application Service Config"""
from __future__ import annotations

import logging
from dataclasses import dataclass
import os

from discover.domain.entity.config.dataset import DatasetConfig
from discover.domain.entity.config.task import TaskConfig
from discover.domain.entity.context.service import ServiceContext
from discover.domain.exception.config import InvalidConfigException
from discover.domain.value_objects.lifecycle import DataPrepStage, Phase, Stage
from discover.infra.repo.factory import ReviewRepoFactory

# ------------------------------------------------------------------------------------------------ #
repo_factory = ReviewRepoFactory()


# ------------------------------------------------------------------------------------------------ #
#                                         CONTEXT                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQAContext(ServiceContext):
    """
    Context specific to the DQA phase of the data preparation process.

    Attributes:
        phase (Phase): The current phase of the pipeline, default is Phase.DATAPREP.
        stage (Stage): The specific stage within the phase, default is DataPrepStage.DQA.
    """

    phase: Phase = Phase.DATAPREP
    stage: Stage = DataPrepStage.DQA

# ------------------------------------------------------------------------------------------------ #
#                                     DATASET CONFIG                                               #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQASourceDatasetConfig(DatasetConfig):
    """
    Configuration class for the source dataset in the Data Quality Assessment (DQA) pipeline.

    This class extends the `DatasetConfig` and provides additional configuration parameters
    specific to the DQA process. It includes service context information, the current stage of
    the data pipeline, and the name of the dataset being processed.

    Attributes:
        service_context (ServiceContext): The context in which the DQA is performed, providing
            necessary service-related metadata. Defaults to `DQAContext()`.
        stage (Stage): The current stage of the data preparation pipeline. Defaults to
            `DataPrepStage.INGEST`.
        name (str): The name of the dataset being processed. Defaults to "reviews".
    """

    service_context: ServiceContext = DQAContext()
    stage: Stage = DataPrepStage.INGEST
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQATargetDatasetConfig(DatasetConfig):
    """
    Configuration class for target data in the Data Quality Assessment (DQA) phase.

    This class extends `DatasetConfig` and provides default values for the DQA process,
    specifically for configuring the target data during the DQA stage. It is designed
    to manage the target dataset settings used during the Data Quality Assessment
    phase of the data preparation pipeline.

    Attributes:
    -----------
    service_context (ServiceContext): The context in which the DQA is performed, providing
            necessary service-related metadata. Defaults to `DQAContext()`.

    stage : Stage, default=DataPrepStage.DQA
        The stage of the data preparation process, set to `DQA`, representing the Data Quality
        Assessment stage of the pipeline.

    name : str, default="reviews"
        The name of the target dataset being processed, set to "reviews" by default, representing
        the dataset to which the DQA process will be applied.

    Inherits:
    ---------
    DatasetConfig:
        Inherits all attributes and methods from the `DatasetConfig` class, which manages configuration
        for data structures, file formats, partitioning, and other dataset-specific details.
    """

    service_context: ServiceContext = DQAContext()
    stage: Stage = DataPrepStage.DQA
    name: str = "reviews"


# ------------------------------------------------------------------------------------------------ #
#                                   TASK CONFIG                                                    #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DQATaskConfig(TaskConfig):
    """
    Configuration class for tasks within the Data Quality Assessment (DQA) pipeline.

    This class extends `TaskConfig` and adds configuration for handling specific columns in a dataset,
    particularly for text processing tasks. It also includes a validation method to ensure that the
    configuration parameters are properly set.

    Attributes:
        text_column (str): The name of the column in the dataset that contains the text data.
            Defaults to "content".
        new_column_name (str): The name of the new column that will be created as part of the task.
            Defaults to an empty string, which must be set explicitly before running the task.

    Methods:
        validate(): Validates the configuration to ensure that `text_column` and `new_column_name` are
            properly set and of the correct types. Raises `InvalidConfigException` if validation fails.
    """

    text_column: str = "content"
    new_column_name: str = ""

    def _validate(self) -> list:
        """
        Validates the configuration for this task. Ensures that `text_column` and `new_column_name`
        are both strings, and that `new_column_name` is not an empty string.

        Raises:
            InvalidConfigException: If any validation checks fail, an exception is raised with a
            detailed error message.
        """
        super()._validate()  # Calling the parent class validation
        errors = []

        if not isinstance(self.text_column, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string instance for text_column. Encountered {type(self.text_column).__name__}."
            )
        if not isinstance(self.new_column_name, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string instance for new_column_name. Encountered {type(self.new_column_name).__name__}."
            )
        if len(self.new_column_name) == 0:
            errors.append(
                f"Invalid {self.__class__.__name__}. The member new_column_name has not been set."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    """
    Configuration class for detecting duplicate rows within a dataset in the Data Quality Assessment (DQA) pipeline.

    This class extends `DQATaskConfig` and predefines the `new_column_name` attribute to "is_duplicate",
    which indicates whether a row is a duplicate.

    Attributes:
        new_column_name (str): The name of the column that will store the result of the duplicate detection.
            Defaults to "is_duplicate".
    """

    new_column_name: str = "is_duplicate"
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectNullValuesTaskConfig(DQATaskConfig):
    new_column_name: str = "has_null"
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectNonEnglishTaskConfig(DQATaskConfig):
    new_column_name: str = "non_english"
    fasttext_model_filepath: str = "models/language_detection/lid.176.ftz"
    n_jobs: int = 12

    def _validate(self) -> list:
        """"""
        super()._validate()  # Calling the parent class validation
        errors = []

        if not isinstance(self.fasttext_model_filepath, str):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a string instance for fasttext_model_filepath. Encountered {type(self.fasttext_model_filepath).__name__}."
            )
        if not os.path.exists(self.fasttext_model_filepath):
            errors.append(
                f"Invalid {self.__class__.__name__}. Fasttext language model not found at {self.fasttext_model_filepath}."
            )

        if not isinstance(self.n_jobs, int):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected an integer instance for n_jobs. Encountered {type(self.text_column).__name__}."
            )
        if self.n_jobs < 2 or self.n_jobs > 24:
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected value in [2,24]. Encountered {self.n_jobs}."
            )

        if errors:
            error_msg = "\n".join(errors)
            logging.error(error_msg)
            raise InvalidConfigException(error_msg)

# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectEmojiTaskConfig(DQATaskConfig):
    new_column_name: str = "has_emoji"
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectSpecialCharacterTaskConfig(DQATaskConfig):
    new_column_name: str = "has_excessive_special_chars"
    threshold: float = 0.2

    def _validate(self) -> list:
        super()._validate()
        errors = []
        if not isinstance(self.threshold, float):
            errors.append(
                f"Invalid {self.__class__.__name__}. Expected a float instance for threshold. Encountered {type(self.threshold).__name__}."
                )
        if self.threshold<= 0 or self.threshold >= 1:
            errors.append(
                    f"Invalid {self.__class__.__name__}. The threshold value, {self.threshold}, must be in (0,1)."
                )
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DetectDuplicateRowTaskConfig(DQATaskConfig):
    new_column_name: str =

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

    source_data_config : DatasetConfig, default=DQASourceDatasetConfig()
        Configuration object for the source data, detailing the source dataset for the DQA process.

    target_data_config : DatasetConfig, default=DQATargetDatasetConfig()
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
    source_data_config: DatasetConfig = DQASourceDatasetConfig()
    target_data_config: DatasetConfig = DQATargetDatasetConfig()
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
