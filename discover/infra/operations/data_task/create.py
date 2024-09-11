#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/operations/data_task/create.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 03:17:23 pm                                                   #
# Modified   : Wednesday September 11th 2024 01:16:51 pm                                           #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import warnings

import numpy as np
import pandas as pd

from discover.domain.service.base.task import Task
from discover.domain.value_objects.lifecycle import Stage
from discover.infra.config.config import Config

# ------------------------------------------------------------------------------------------------ #
warnings.simplefilter(action="ignore", category=FutureWarning)


# ------------------------------------------------------------------------------------------------ #
class CreateDatasetTask(Task):
    """A task to create dataset for an environment.

    Args:
        config (Dict[str,str]): Dataset configuration.
        text_col (str): Column containing review text.
        random_state (int): Seed for pseudo randomization
    """

    __STAGE = Stage.RAW

    def __init__(
        self,
        config_cls: type[Config] = Config,
        text_col: str = "content",
        test_sample: float = 0.1,
        random_state: int = None,
    ) -> None:
        super().__init__(stage=self.__STAGE)
        self._config = config_cls()
        self._text_col = text_col
        self._test_sample = test_sample
        self._random_state = random_state

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        """Create datasets for different environments.

        This method creates datasets for production, development, and testing environments
        based on the input DataFrame. The production dataset is a full copy of the input data,
        while the development and testing datasets are sampled from the input data to create
        representative subsets. Random sampling is used to ensure that the dev and test sets
        contain a fraction (`frac`) of the original data while preserving its distribution.
        The `random_state` parameter allows for reproducibility of the sampling process.

        Args:
            data (pd.DataFrame): Input DataFrame containing the dataset.

        """
        # Obtain the current environment for which the datasets are being created.
        env = self._config.get_environment()

        # Obtain the dataset sample size from configuration
        dataset_config = self._config.get_config(section="dataset")

        # Sample as per the configuration file.
        df_sample = data.sample(
            frac=dataset_config.frac, replace=False, random_state=self._random_state
        )
        # Add test samples if current environment is test.
        if env.lower() == "test":
            df_sample = self._add_noise(df=df_sample)

        return df_sample

    def _add_noise(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds additional test samples to the dataset if test environment"""

        print("Adding noise to samples.")
        review_text = "%#$#some (*)^%$# https://www.url.com &*^%$#@ (555) 555-1212. myemail@gmail.com fuck, sh!t (#&$) :-) ;-)"
        # Get random sample
        sample = df.sample(n=1)
        # Add corrupting text content
        sample[self._text_col] = review_text
        # Set author to to none
        sample["author"] = "J2"
        # Create copies of the corrupted sample
        samples = pd.DataFrame(np.repeat(sample.values, 5, axis=0))
        # Add column names
        samples.columns = df.columns
        # Combine original and corrupted samples into a new dataframe
        df_new = pd.concat([df, samples], axis=0)
        # Cast columns to be the same data types as the input dataframe.
        for column in df.columns:
            df_new[column] = df_new[column].astype(df[column].dtype)

        return df_new
