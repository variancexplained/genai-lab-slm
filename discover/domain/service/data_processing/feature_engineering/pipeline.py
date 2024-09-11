#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/domain/service/data_processing/feature_engineering/pipeline.py            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday May 30th 2024 12:47:36 pm                                                  #
# Modified   : Wednesday September 11th 2024 02:51:55 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Feature Engineering Module"""
from dataclasses import dataclass, field
from typing import List

import nltk
import numpy as np
import pandas as pd
from nltk import pos_tag
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from pandarallel import pandarallel
from textstat import textstat

from discover.application.pipeline import Pipeline, PipelineBuilder, StageConfig
from discover.data_prep import log_exceptions, task_profiler
from discover.data_prep.io import ReadTask, WriteTask
from discover.domain.service.base.task import Task
from discover.utils.base import Reader, Writer
from discover.utils.cast import CastPandas
from discover.utils.io import PandasReader, PandasWriter
from discover.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
nltk.download("punkt")
nltk.download("stopwords")
nltk.download("averaged_perceptron_tagger")
# ------------------------------------------------------------------------------------------------ #
pandarallel.initialize(progress_bar=False, nb_workers=12, verbose=0)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FeatureEngineeringConfig(StageConfig):
    """Encapsulates the configuration for feature engineering."""

    name: str = "FeatureEngineering"
    source_directory: str = "03_clean/reviews"
    source_filename: str = None
    target_directory: str = "04_features/reviews"
    target_filename: str = None
    partition_cols: str = "category"
    force: bool = False
    hash_size: int = 10
    features_to_drop: List = field(default_factory=lambda: ["category_id"])
    datatypes: dict = field(
        default_factory=lambda: {
            "id": "string",
            "app_id": "string",
            "app_name": "string",
            "category": "category",
            "author": "string",
            "rating": "int64",
            "content": "string",
            "vote_count": "int64",
            "vote_sum": "int64",
            "date": "datetime64[ms]",
            "dt_year": "int",
            "dt_month": "str",
            "dt_day": "str",
            "dt_year_month": "str",
            "dt_ymd": "str",
        }
    )


# ------------------------------------------------------------------------------------------------ #
#                               FEATURE ENGINEERING                                                #
# ------------------------------------------------------------------------------------------------ #
class FeatureEngineer(PipelineBuilder):
    """Encapsulates the feature engineering tasks

    Attributes:
        data (pd.DataFrame): The enriched dataset

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
        parse = ParseDatesTask()

        drop = DropFeaturesTask(features_to_drop=self.config.features_to_drop)

        cast = CastDatatypesTask(dtype_mapping=self.config.datatypes)

        basic_text = BasicTextFeaturesTask()
        lexical = LexicalFeaturesTask()
        readability = ReadabilityMetricsTask()

        # Add tasks to pipeline...
        pipe.add_task(load)
        pipe.add_task(parse)
        pipe.add_task(drop)
        pipe.add_task(cast)
        pipe.add_task(basic_text)
        pipe.add_task(lexical)
        pipe.add_task(readability)
        pipe.add_task(save)
        return pipe


# ------------------------------------------------------------------------------------------------ #
#                                 PARSE DATES TASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class ParseDatesTask(Task):
    """Parse dates into years, months, day of month, day of week, etc..."""

    def __init__(self) -> None:
        super().__init__()

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task, adding parsed elements of the date to the DataFrame

        Args:
            data (pd.DataFrame): Review data
        """

        # Calculate the maximum date in the dataset as the date of extraction.
        extraction_date = data["date"].max()

        # Compute the difference in days between extraction_date and each review date in days.
        data["dt_review_age"] = (extraction_date - data["date"]).dt.days

        # Adding existing date-related features
        data["dt_year"] = data["date"].dt.strftime("%Y")
        data["dt_month"] = data["date"].dt.strftime("%B")
        data["dt_day"] = data["date"].dt.strftime("%A")
        data["dt_year_month"] = data["date"].dt.strftime("%Y-%m")
        data["dt_ymd"] = data["date"].dt.strftime("%Y-%m-%d")

        return data


# ------------------------------------------------------------------------------------------------ #
#                               BASIC TEXT FEATURES TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class BasicTextFeaturesTask(Task):
    """Task to add basic text features to the dataset.

    Basic Text Features:
        - Sentence Count: Total number of sentences.
        - Word Count: Total number of words.
        - Character Count: Total number of characters excluding spaces.
        - Sentence Density: Average number of words per sentence.
        - Word Density: Average length of words.
        - Punctuation Count: Total number of punctuation marks.
        - Stopwords Count: Total number of stopwords.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        stop_words = set(stopwords.words("english"))

        data["text_sentence_count"] = data["content"].parallel_apply(
            lambda x: len(sent_tokenize(x))
        )
        data["text_word_count"] = data["content"].parallel_apply(
            lambda x: len(word_tokenize(x))
        )
        data["text_char_count"] = data["content"].parallel_apply(
            lambda x: len(x.replace(" ", ""))
        )
        data["text_punctuation_count"] = data["content"].parallel_apply(
            lambda x: sum([1 for char in x if char in ".,;!?"])
        )
        data["text_stopwords_count"] = data["content"].parallel_apply(
            lambda x: sum(
                [1 for word in word_tokenize(x) if word.lower() in stop_words]
            )
        )
        data["text_word_density"] = data["text_char_count"] / data["text_word_count"]
        data["text_sentence_density"] = (
            data["text_word_count"] / data["text_sentence_count"]
        )
        return data


# ------------------------------------------------------------------------------------------------ #
#                                LEXICAL FEATURES TASK                                             #
# ------------------------------------------------------------------------------------------------ #
class LexicalFeaturesTask(Task):
    """Task to add lexical features to the dataset.

    Lexical Features:
        - Unique Word Count: Number of unique words in the text.
        - Type-Token Ratio (TTR): Ratio of unique words to the total number of words.
        - Lexical Density: Proportion of content words (nouns, verbs, adjectives, adverbs) to the total number of words.
        - Average Word Length: Average length of words in the text.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        data["lexical_unique_word_count"] = data["content"].parallel_apply(
            lambda x: len(set(word_tokenize(x)))
        )
        data["lexical_type_token_ratio"] = (
            data["lexical_unique_word_count"] / data["basic_word_count"]
        )
        data["lexical_density"] = data["content"].parallel_apply(
            lambda x: len(
                [
                    word
                    for word, pos in pos_tag(word_tokenize(x))
                    if pos.startswith(("N", "V", "J", "R"))
                ]
            )
            / len(word_tokenize(x))
        )
        data["lexical_avg_word_length"] = data["content"].parallel_apply(
            lambda x: np.mean([len(word) for word in word_tokenize(x)])
        )
        return data


# ------------------------------------------------------------------------------------------------ #
#                              READABILITY FEATURES TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class ReadabilityMetricsTask(Task):
    """Task to add readability metrics to the dataset.

    Readability Metrics:
        - Flesch Reading Ease: Score indicating how easy the text is to read.
        - Flesch-Kincaid Grade Level: Indicates the U.S. school grade level needed to understand the text.
        - Gunning Fog Index: Estimates the years of formal education needed to understand the text.
        - SMOG Index: Measure of readability that estimates the years of education needed to understand a piece of writing.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        data["readability_flesch_reading_ease"] = data["content"].parallel_apply(
            lambda x: textstat.flesch_reading_ease(x)
        )
        data["readability_flesch_kincaid_grade"] = data["content"].parallel_apply(
            lambda x: textstat.flesch_kincaid_grade(x)
        )
        data["readability_gunning_fog"] = data["content"].parallel_apply(
            lambda x: textstat.gunning_fog(x)
        )
        data["readability_smog_index"] = data["content"].parallel_apply(
            lambda x: textstat.smog_index(x)
        )
        return data


# ------------------------------------------------------------------------------------------------ #
#                               REMOVE FEATURES TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DropFeaturesTask(Task):
    """Drops designated features from the dataset..."""

    def __init__(self, features_to_drop: list = []) -> None:
        super().__init__()
        self._features_to_drop = features_to_drop

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task, dropping designated features from the dataset.

        Args:
            data (pd.DataFrame): Review data
        """
        data = data.drop(columns=self._features_to_drop)
        return data


# ------------------------------------------------------------------------------------------------ #
#                               CAST DATA TYPES TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class CastDatatypesTask(Task):
    """Casts data types on Dataset."""

    def __init__(
        self, dtype_mapping: dict, cast_cls: type[CastPandas] = CastPandas
    ) -> None:
        super().__init__()
        self._dtype_mapping = dtype_mapping
        self._cast_cls = cast_cls

    @log_exceptions()
    @task_profiler
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task, dropping designated features from the dataset.

        Args:
            data (pd.DataFrame): Review data
        """
        cast = self._cast_cls()
        data = cast.apply(data=data, datatypes=self._dtype_mapping)
        return data
