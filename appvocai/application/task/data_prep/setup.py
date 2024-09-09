#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/application/task/data_prep/setup.py                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 28th 2024 03:17:23 pm                                                   #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import warnings
from typing import Dict

import numpy as np
import pandas as pd
from appvocai.application.base import Task

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

    def __init__(
        self,
        config: Dict[str, str],
        text_col: str = "content",
        test_sample: float = 0.1,
        random_state: int = 22,
    ) -> None:
        super().__init__()
        self._config = config
        self._text_col = text_col
        self._test_sample = test_sample
        self._random_state = random_state

    def run_task(self, data: pd.DataFrame) -> None:
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
        env = self._config["env"].lower()

        # Sample as per the configuration file.
        df_sample = data.sample(
            frac=self._config["dataset"]["frac"],
            replace=False,
            random_state=self._config["dataset"]["random_state"],
        )
        # Add test samples if current environment is test.
        if env.lower() == "test":
            df_sample = self._add_test_samples(df=df_sample)

        return df_sample

    def is_setup(self) -> bool:
        """Returns true if the dataset has been setup"""
        return os.path.exists(self._config["dataset"]["filepath"])

    def _add_test_samples(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adds additional test samples to the dataset if test environment"""

        print("Sampling test set.")
        df = df.sample(frac=0.1, random_state=self._random_state)

        print("Adding test samples.")
        review_text = "%#$#some (*)^%$# https://www.url.com &*^%$#@ (555) 555-1212. myemail@gmail.com fuck, sh!t (#&$) :-) ;-)"
        # Get random sample
        sample = df.sample(n=1)
        # Add corrupting text content
        sample[self._text_col] = review_text
        # Set author to to none
        sample["author"] = None
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
