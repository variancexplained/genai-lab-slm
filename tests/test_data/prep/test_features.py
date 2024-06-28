#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /tests/test_data/prep/test_features.py                                              #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday June 1st 2024 09:02:20 pm                                                  #
# Modified   : Sunday June 16th 2024 03:47:47 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import inspect
import logging
from datetime import datetime

import pandas as pd
import pytest

from appinsight.data_prep.feature import (
    AnonymizeAuthorsTask,
    BasicTextFeaturesTask,
    CastDatatypesTask,
    DropFeaturesTask,
    FeatureEngineer,
    FeatureEngineeringConfig,
    LexicalFeaturesTask,
    ParseDatesTask,
    ReadabilityMetricsTask,
)

# ------------------------------------------------------------------------------------------------ #
# pylint: disable=missing-class-docstring, line-too-long
# ------------------------------------------------------------------------------------------------ #
# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)
# ------------------------------------------------------------------------------------------------ #
double_line = f"\n{100 * '='}"
single_line = f"\n{100 * '-'}"


@pytest.mark.feature
class TestFeatureEngineeringTasks:  # pragma: no cover
    # ============================================================================================ #
    def test_date_features(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = ParseDatesTask()
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert "dt_review_age" in data.columns
        assert "dt_year" in data.columns
        assert "dt_month" in data.columns
        assert "dt_day" in data.columns
        assert "dt_year_month" in data.columns
        assert "dt_ymd" in data.columns
        assert data.isna().sum().sum() == 0
        logging.debug(data.info())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_anonymization(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = AnonymizeAuthorsTask()
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert data.isna().sum().sum() == 0
        logging.debug(data.head())

        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_basic_text_features(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = BasicTextFeaturesTask()
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert "basic_sentence_count" in data.columns
        assert "basic_word_count" in data.columns
        assert "basic_char_count" in data.columns
        assert "basic_punctuation_count" in data.columns
        assert "basic_stopwords_count" in data.columns
        assert data.isna().sum().sum() == 0
        logging.debug(data.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_lexical_features_task(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = BasicTextFeaturesTask()
        data = task.execute(data=data_clean)
        task = LexicalFeaturesTask()
        data = task.execute(data=data)
        assert isinstance(data, pd.DataFrame)
        assert "lexical_type_token_ratio" in data.columns
        assert "lexical_unique_word_count" in data.columns
        assert "lexical_avg_word_length" in data.columns
        assert data.isna().sum().sum() == 0
        logging.debug(data.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_readability_task(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = ReadabilityMetricsTask()
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert "readability_flesch_reading_ease" in data.columns
        assert "readability_flesch_kincaid_grade" in data.columns
        assert "readability_gunning_fog" in data.columns
        assert "readability_smog_index" in data.columns
        assert data.isna().sum().sum() == 0
        logging.debug(data.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_cast_datatypes(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = CastDatatypesTask(
            dtype_mapping={
                "id": "string",
                "app_id": "string",
                "app_name": "string",
                "category": "category",
                "author": "string",
                "rating": "int64",
                "title": "string",
                "content": "string",
                "review_length": "int64",
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
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert data["date"].dtype == "datetime64[ms]"
        logging.debug(data.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_drop_features(self, data_clean, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        task = DropFeaturesTask(features_to_drop=["category_id"])
        data = task.execute(data=data_clean)
        assert isinstance(data, pd.DataFrame)
        assert "category_id" not in data.columns
        logging.debug(data.head())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)


@pytest.mark.feature
class TestFeatureEngineer:  # pragma: no cover
    # ============================================================================================ #
    def test_feature_engineering(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = FeatureEngineeringConfig(force=True)
        fe = FeatureEngineer(config=config)
        data = fe.execute()
        assert isinstance(data, pd.DataFrame)
        logging.debug(data.info())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)

    # ============================================================================================ #
    def test_already_completed(self, caplog):
        start = datetime.now()
        logger.info(
            f"\n\nStarted {self.__class__.__name__} {inspect.stack()[0][3]} at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(double_line)
        # ---------------------------------------------------------------------------------------- #
        config = FeatureEngineeringConfig(force=False)
        fe = FeatureEngineer(config=config)
        data = fe.execute()
        assert isinstance(data, pd.DataFrame)
        assert "category_id" not in (data.columns)
        logging.debug(data.info())
        # ---------------------------------------------------------------------------------------- #
        end = datetime.now()
        duration = round((end - start).total_seconds(), 1)

        logger.info(
            f"\n\nCompleted {self.__class__.__name__} {inspect.stack()[0][3]} in {duration} seconds at {start.strftime('%I:%M:%S %p')} on {start.strftime('%m/%d/%Y')}"
        )
        logger.info(single_line)
