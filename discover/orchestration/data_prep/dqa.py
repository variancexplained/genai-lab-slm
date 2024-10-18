#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/orchestration/data_prep/dqa.py                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday October 17th 2024 09:34:20 pm                                              #
# Modified   : Friday October 18th 2024 01:42:42 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Data Quality Analysis Module"""
import logging
import os
import re
from collections import Counter
from typing import AnyStr, List, Optional, Union

import emoji
import fasttext
import numpy as np
import pandas as pd
from dependency_injector.wiring import Provide, inject
from joblib import Parallel, delayed
from profanity_check import predict
from tqdm import tqdm

from discover.assets.dataset import Dataset
from discover.assets.idgen import AssetIDGen
from discover.container import DiscoverContainer
from discover.core.flow import DataPrepStageDef, PhaseDef
from discover.infra.persistence.repo.dataset import DatasetRepo
from discover.infra.service.logging.stage import stage_logger
from discover.infra.service.logging.task import task_logger
from discover.infra.utils.data.dataframe import split_dataframe
from discover.orchestration.base.stage import Stage
from discover.orchestration.base.task import Task


# ------------------------------------------------------------------------------------------------ #
#                                     DQA STAGE                                                    #
# ------------------------------------------------------------------------------------------------ #
class DQAStage(Stage):
    """
    A stage class for preparing datasets, handling loading, processing, and saving of data.

    The `DataPrepStage` class orchestrates the execution of data preparation tasks,
    including loading source datasets, applying a series of tasks, and saving the processed
    data to a destination. It uses a repository for dataset persistence and can be configured
    to force execution even if the destination dataset already exists.

    Parameters
    ----------
    source_config : dict
        Configuration for the source dataset, including details like phase, stage, and name.
    destination_config : dict
        Configuration for the destination dataset, including details like phase, stage, and name.
    tasks : List[Task]
        A list of tasks to execute as part of the data preparation stage.
    force : bool, optional
        Whether to force execution if the destination dataset endpoint already exists (default is False).
    repo : DatasetRepo, optional
        A repository for dataset persistence, injected via dependency injection (default is `DiscoverContainer.repo.dataset_repo`).
    **kwargs : dict
        Additional keyword arguments for stage configuration.

    Attributes
    ----------
    _repo : DatasetRepo
        The repository instance used for dataset persistence.
    _source_asset_id : str
        The generated asset ID for the source dataset based on the configuration.
    _destination_asset_id : str
        The generated asset ID for the destination dataset based on the configuration.
    _logger : logging.Logger
        Logger instance for logging events related to the data preparation stage.

    Methods
    -------
    run() -> None
        Executes the stage by loading the source dataset, applying tasks, and saving the result.
    _create_destination_dataset(data: Union[pd.DataFrame, pyspark.sql.DataFrame]) -> Dataset
        Creates the destination dataset with the processed data and configuration details.
    _load_source_dataset() -> Dataset
        Loads the source dataset from the repository using the source asset ID.
    _save_destination_dataset(dataset: Dataset) -> None
        Saves the processed dataset to the repository using the destination asset ID.
    _endpoint_exists(asset_id: str) -> bool
        Checks if the dataset endpoint already exists in the repository.

    Examples
    --------
    >>> source_config = {'phase': 'preprocessing', 'stage': 'normalization', 'name': 'raw_data'}
    >>> destination_config = {'phase': 'preprocessing', 'stage': 'normalized', 'name': 'cleaned_data'}
    >>> tasks = [Task1(), Task2()]
    >>> data_prep_stage = DataPrepStage(
    ...     source_config=source_config,
    ...     destination_config=destination_config,
    ...     tasks=tasks,
    ...     force=True
    ... )
    >>> data_prep_stage.run()

    Notes
    -----
    The `DataPrepStage` class leverages dependency injection to retrieve a dataset repository instance.
    It ensures that datasets are properly loaded and saved based on the specified configurations.
    """

    @inject
    def __init__(
        self,
        source_config: dict,
        destination_config: dict,
        tasks: List[Task],
        force: bool = False,
        repo: DatasetRepo = Provide[DiscoverContainer.repo.dataset_repo],
        **kwargs,
    ) -> None:
        super().__init__(
            source_config=source_config,
            destination_config=destination_config,
            tasks=tasks,
            force=force,
        )
        self._repo = repo

        self._destination_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._destination_config.asset_type,
            phase=PhaseDef.from_value(value=self._destination_config.phase),
            stage=DataPrepStageDef.from_value(value=self._destination_config.stage),
            name=self._destination_config.name,
        )

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @property
    def phase(self) -> PhaseDef:
        return PhaseDef.from_value(value=self._destination_config.phase)

    @property
    def stage(self) -> PhaseDef:
        return DataPrepStageDef.from_value(value=self._destination_config.stage)

    @stage_logger
    def run(self) -> str:
        """Executes the stage by loading the source dataset, applying tasks, and saving the result.

        Returns:
            asset_id (str): Returns the asset_id for the asset created.
        """
        if (
            self._endpoint_exists(asset_id=self._destination_asset_id)
            and not self._force
        ):
            return self._destination_asset_id
        else:
            if self._repo.exists(asset_id=self._destination_asset_id):
                self._repo.remove(asset_id=self._destination_asset_id)

            data = self._load_source_data()
            data.sort_index(inplace=True)
            results = []

            for task in self._tasks:
                result = task.run(data=data)
                results.append(result)

            results = pd.concat(results, axis=1)
            results = pd.concat((data["id"], results), axis=1)
            dataset = self._create_destination_dataset(data=results)

            self._save_destination_dataset(dataset=dataset)

            return self._destination_asset_id

    def _endpoint_exists(self, asset_id: str) -> bool:
        """Checks if the dataset endpoint already exists in the repository."""
        return self._repo.exists(asset_id=asset_id)

    def _load_source_data(self) -> pd.DataFrame:
        """Loads source data."""
        source_asset_id = AssetIDGen.get_asset_id(
            asset_type=self._source_config.asset_type,
            phase=PhaseDef.from_value(value=self._source_config.phase),
            stage=DataPrepStageDef.from_value(value=self._source_config.stage),
            name=self._source_config.name,
        )
        return self._repo.get(asset_id=source_asset_id).content

    def _create_destination_dataset(self, data: pd.DataFrame) -> Dataset:
        """Creates the destination dataset with the processed data and configuration details."""
        return Dataset(
            phase=PhaseDef.from_value(self._destination_config.phase),
            stage=DataPrepStageDef.from_value(self._destination_config.stage),
            name=self._destination_config.name,
            content=data,
            nlp=self._destination_config.nlp,
            distributed=self._destination_config.distributed,
        )

    def _remove_destination_dataset(self) -> None:
        self._repo.remove(asset_id=self._destination_asset_id)

    def _save_destination_dataset(self, dataset: Dataset) -> None:
        """Saves the processed dataset to the repository using the destination asset ID."""
        self._repo.add(dataset=dataset)


# ------------------------------------------------------------------------------------------------ #
#                                       ENTROPY                                                    #
# ------------------------------------------------------------------------------------------------ #
class EntropyTask(Task):
    """ """

    def __init__(
        self, text_column: str = "content", dqa_column: str = "entropy"
    ) -> None:
        """"""
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """"""
        result = (
            data[self._text_column].parallel_apply(self._calculate_entropy).to_frame()
        )
        result = result.rename(columns={self._text_column: self._dqa_column})
        result.sort_index(inplace=True)
        return result

    @staticmethod
    def _calculate_entropy(text: str) -> float:
        # Count frequency of each character in the text
        freq = Counter(text)

        # Calculate the probabilities of each character
        total_chars = len(text)
        probabilities = [count / total_chars for count in freq.values()]

        # Calculate Shannon entropy
        entropy = -sum(p * np.log2(p) for p in probabilities if p > 0)

        return entropy


# ------------------------------------------------------------------------------------------------ #
#                                       DUPLICATE                                                  #
# ------------------------------------------------------------------------------------------------ #
class DetectDuplicateTask(Task):
    """
    A task that detects duplicate rows in a pandas DataFrame based on specified column(s) and adds a column flagging duplicates.

    Args:
        dqa_column (str): The name of the column to store the duplicate flag results. The new column will contain boolean values indicating whether the row is a duplicate.
        column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) used to detect duplicates. If None, all columns are considered. Defaults to None.

    Attributes:
        _column_names (Optional[Union[List[AnyStr], AnyStr]]): The column(s) to check for duplicates.
        _dqa_column (str): The name of the column where the duplicate flag is stored.
    """

    def __init__(
        self,
        dqa_column: str,
        column_names: Optional[Union[List[AnyStr], AnyStr]] = None,
    ) -> None:
        """
        Initializes the DetectDuplicateTask with the specified column(s) to check for duplicates and the name of the column to store the results.

        Args:
            dqa_column (str): The column name where the duplicate flag will be stored.
            column_names (Optional[Union[List[AnyStr], AnyStr]], optional): The column(s) to use for detecting duplicates. If None, the entire row will be checked for duplicates. Defaults to None.
        """
        super().__init__()
        self._column_names = column_names
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detects duplicate rows based on the specified columns and creates a new column to flag duplicates.

        Args:
            data (pd.DataFrame): The input DataFrame to check for duplicates.

        Returns:
            pd.DataFrame: A DataFrame with a new column indicating whether each row is a duplicate, based on the specified columns.
        """
        result = (
            data[self._text_column].parallel_apply(self._calculate_entropy).to_frame()
        )
        result = (
            data.duplicated(subset=self._column_names, keep="first")
            .to_frame()
            .rename(columns={0: self._dqa_column})
        )
        result.sort_index(inplace=True)
        return result


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT NULL VALUES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectNullValuesTask(Task):
    """A task to mark incomplete rows in a DataFrame.

    Attributes:
        new_column_name (str): The name of the column to add, indicating an incomplete row.
    """

    def __init__(
        self,
        dqa_column: str = "has_null",
    ):
        super().__init__()
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """
        result = data.isnull().any(axis=1)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT INVALID RATINGS                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidRatingsTask(Task):
    """"""

    def __init__(
        self, dqa_column: str = "invalid_rating", rating_column: str = "rating"
    ):
        super().__init__()
        self._dqa_column = dqa_column
        self._rating_column = rating_column

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Mark incomplete rows in the DataFrame.

        Parameters:
            data (pd.DataFrame): The input DataFrame.

        Returns:
            pd.DataFrame: The DataFrame with an additional column indicating whether each row is incomplete.
        """
        result = data[self._rating_column].parallel_apply(self._rating_invalid)
        result = result.rename(columns={self._rating_column: self._dqa_column})
        result.sort_index(inplace=True)
        return result

    def _rating_invalid(self, rating: int) -> bool:
        """Indicates whether the rating is on the five point scale."""
        return rating < 0 or rating > 5


# ------------------------------------------------------------------------------------------------ #
#                            DETECT NON-ENGLISH TASK                                               #
# ------------------------------------------------------------------------------------------------ #
# Standalone function for processing a chunk
def process_chunk(chunk, text_column, new_column, model_path):
    model = fasttext.load_model(model_path)  # Load the model in each worker
    return chunk[text_column].apply(lambda text: is_non_english(text, model))


# Standalone function to determine if text is non-English
def is_non_english(text, model):
    try:
        predictions = model.predict(text)
        return predictions[0][0] != "__label__en"
    except Exception as e:
        # Use a simple print statement for error logging in parallel function
        print(f"Error in language detection: {e}")
        return False


class DetectNonEnglishTask(Task):
    """Detects which reviews contain non-English text.

    Args:
        text_column (str): Name of the column containing text to be analyzed.
        new_column_name (str): Name of the new column to be created indicating whether the text is in English or not.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_non_english",
        n_jobs: int = 12,
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._n_jobs = n_jobs
        # Load pre-trained FastText language identification model
        self._model_filepath = os.getenv("FASTTEXT_MODEL")

    @task_logger
    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Executes the task to detect non-English text in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text is in English.
        """

        # Split data into chunks
        chunks = split_dataframe(data=data, n=self._n_jobs)
        self._logger.debug(f"Data split into {len(chunks)} chunks")

        # Process chunks in parallel using joblib
        results = Parallel(n_jobs=self._n_jobs)(
            delayed(process_chunk)(
                chunk, self._text_column, self._dqa_column, self._model_filepath
            )
            for chunk in tqdm(chunks)
        )

        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMOJI TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmojiTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_has_emoji",
    ):
        """
        Initializes the DetectEmojiTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._text_column].parallel_apply(self._contains_emoji)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    @staticmethod
    def _contains_emoji(text):
        """Detects if the given text contains any emojis.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains emojis, False otherwise.
        """
        return any(char in emoji.EMOJI_DATA for char in text)


# ------------------------------------------------------------------------------------------------ #
#                            DETECT SPECIAL CHARACTERS TASK                                        #
# ------------------------------------------------------------------------------------------------ #
class DetectSpecialCharacterTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_excessive_special_chars",
        threshold: float = 0.2,
    ):
        """Initializes the DetectSpecialCharacterTask with the column names.

        Class flags rows with excessive number of special characters in the review text. Excessive
        is defined in terms of a proportion of overall review length.
        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating whether the text contains emojis or not.
            threshold (float): Proportion of review text with special characters, above which, is considered
                excessive
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._threshold = threshold

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect emojis in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains emojis.

        """

        result = data[self._text_column].parallel_apply(
            self._contains_excessive_special_chars
        )

        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    def _contains_excessive_special_chars(self, text):
        """Detects if the given text contains an excessive number of special characters.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains excessive special characters, False otherwise.
        """
        special_chars = re.findall(r"[^\w\s]", text)
        return (
            len(special_chars) / len(text) > self._threshold if len(text) > 0 else False
        )


# ------------------------------------------------------------------------------------------------ #
#                               DETECT INVALID DATES TASK                                          #
# ------------------------------------------------------------------------------------------------ #
class DetectInvalidDatesTask(Task):
    def __init__(
        self, date_column: str, dqa_column: str, start_date: int, end_date: int
    ):
        """
        Detects dates that are outside the range of 2008-2023

        Args:
            new_column_name (str): Name of the new column to be created indicating whether the rating is valid or not.
        """
        super().__init__()
        self._date_column = date_column
        self._dqa_column = dqa_column
        self._start_date = start_date
        self._end_date = end_date

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect invalid dates and to add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains invalid dates.

        """

        result = data[self._date_column].parallel_apply(self._date_invalid)

        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    def _date_invalid(self, date: np.datetime64) -> bool:
        """Indicates whether the rating is on the five point scale."""
        date = np.datetime64(date)

        return date < np.datetime64(str(self._start_date)) or date > np.datetime64(
            str(self._end_date)
        )


# ------------------------------------------------------------------------------------------------ #
#                                 DETECT PROFANITY                                                 #
# ------------------------------------------------------------------------------------------------ #
def detect_profanity(text):
    """
    Detects if the given text contains any profanity.

    Args:
        text (str): The text to be analyzed.

    Returns:
        bool: True if the text contains profanity, False otherwise.
    """
    try:
        return predict([text])[0] == 1
    except Exception as e:
        print(f"Error in profanity detection: {e}")
        return False


# ------------------------------------------------------------------------------------------------ #
class DetectProfanityTask(Task):
    """Detects profanity in the designated column of a dataframe.

    Note: This class leverages the multiprocessing module. To avoid unintended behavior and
    recursively spawning processes, this should be executed with the __main__ shield. This
    is not strictly required in a WSL2 virtual machine, but should be refactored behind
    a __main__  shield for cross-platform portability.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_has_profanity",
        n_jobs: int = 12,
    ):
        """
        Initializes the DetectProfanityTask with the column names.

        Args:
            text_column (str): Name of the column containing text to be analyzed.
            new_column_name (str): Name of the new column to be created indicating
                whether the text contains profanity or not.
            n_jobs (int): Number of cpus corresponding to 'jobs' in a concurrent context.
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column
        self._n_jobs = n_jobs

    @task_logger
    def run(self, data: pd.DataFrame):
        """
        Executes the task to detect profanity in the specified column and add a new column with the results.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with an additional column indicating whether the text contains profanity.

        """
        chunks = split_dataframe(data=data, n=100)
        results = [
            chunk.parallel_apply(
                lambda row: detect_profanity(row[self._text_column]), axis=1
            )
            for chunk in tqdm(chunks)
        ]
        # Concatenate the processed chunks
        result = pd.concat(results, axis=0)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT EMAIL TASK                                               #
# ------------------------------------------------------------------------------------------------ #
class DetectEmailTask(Task):
    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "contains_email",
    ):
        """Initializes the DetectSpecialPatternsTask with the column name containing text data.


        Args:
            text_column (str): Name of the column containing text to be analyzed.
        """
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects special patterns (emails, URLs, phone numbers) in the specified text column
        and adds new columns indicating the presence of each pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_email)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    @staticmethod
    def _contains_email(text):
        """
        Detects if the given text contains an email address.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains an email address, False otherwise.
        """
        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        return bool(re.search(email_pattern, text))


# ------------------------------------------------------------------------------------------------ #
#                                  DETECT URL TASK                                                 #
# ------------------------------------------------------------------------------------------------ #
class DetectURLTask(Task):
    """Detects the presence of URLs in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_contains_url",
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_url)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    @staticmethod
    def _contains_url(text):
        """
        Detects if the given text contains a URL.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains a URL, False otherwise.
        """
        url_pattern = r"\b(?:https?://|www\.)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?:/[^\s]*)?\b"
        return bool(re.search(url_pattern, text))


# ------------------------------------------------------------------------------------------------ #
#                               DETECT PHONE NUMBER TASK                                           #
# ------------------------------------------------------------------------------------------------ #
class DetectPhoneNumberTask(Task):
    """Detects the presence of phone numbers in a text column.

    Args:
        text_column (str): Name of column containing text to search.
        new_column_name (str): Name of indicator column created in dataset.
    """

    def __init__(
        self,
        text_column: str = "content",
        dqa_column: str = "b_contains_phone_number",
    ):
        super().__init__()
        self._text_column = text_column
        self._dqa_column = dqa_column

    @task_logger
    def run(self, data: pd.DataFrame):
        """Detects URLs in the specified text column
        and adds new columns indicating the presence of the pattern.

        Args:
            data (pd.DataFrame): A pandas DataFrame containing the data to be processed.

        Returns:
            pd.DataFrame: A pandas DataFrame with additional columns indicating special pattern detection.

        """

        result = data[self._text_column].parallel_apply(self._contains_phone_number)
        result = pd.DataFrame(result, columns=[self._dqa_column])
        result.sort_index(inplace=True)
        return result

    @staticmethod
    def _contains_phone_number(text):
        """
        Detects if the given text contains a phone number.

        Args:
            text (str): The text to be analyzed.

        Returns:
            bool: True if the text contains a phone number, False otherwise.
        """
        phone_number_pattern = r"\b(?:\d{3}[-.]?|\(\d{3}\) )?\d{3}[-.]?\d{4}\b"
        return bool(re.search(phone_number_pattern, text))
