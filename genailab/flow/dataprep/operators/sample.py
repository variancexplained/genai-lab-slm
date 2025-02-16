#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/flow/dataprep/operators/sample.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday February 8th 2025 04:36:44 am                                              #
# Modified   : Saturday February 8th 2025 10:43:03 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Sample DataFrame Module"""
import math

from pyspark.sql import DataFrame

from genailab.flow.base.task import Task
from genailab.infra.service.logging.task import task_logger


# ------------------------------------------------------------------------------------------------ #
class SampleDataFrameTask(Task):
    """
    Samples a Spark DataFrame based on specified confidence level,
    estimated proportion, and margin of error.

    Attributes:
        z (float): Z-score corresponding to the desired confidence level.
                   Defaults to 1.96 (for 95% confidence).
        p (float): Estimated proportion for sample size calculation.
                   Defaults to 0.5 (most conservative estimate).
        moe (float): Desired margin of error (as a decimal).
                   Defaults to 0.01 (1%).
        random_state (int): Random seed for sample reproducibility.
                           Defaults to 55.

    Methods:
        run(data: DataFrame) -> DataFrame: Samples the input DataFrame.
    """

    def __init__(self, z: float = 1.96, p: float = 0.5, moe: float = 0.01, min_sample_size: int = 1024, random_state: int = 55):
        super().__init__()
        self._z = z
        self._p = p
        self._moe = moe
        self._min_sample_size = min_sample_size
        self._random_state = random_state

    @task_logger
    def run(self, data: DataFrame) -> DataFrame:
        """
        Samples the input DataFrame based on the configured parameters.

        Args:
            data (DataFrame): The input PySpark DataFrame.

        Returns:
            DataFrame: A sampled DataFrame.

        Raises:
            ValueError: If the calculated sample size is greater than the
                        actual DataFrame size.
        """
        N = data.count()
        n = min(max(self._z**2 * self._p * (1 - self._p)/ self._moe**2, self._min_sample_size), N)
        n = math.ceil(n)

        self._note = f"Sampled {n} out of {N} observations" if n < N else None


        frac = n / N
        return data.sample(withReplacement=False, fraction=frac, seed=self._random_state)