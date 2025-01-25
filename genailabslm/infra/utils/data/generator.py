#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/infra/utils/data/generator.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday October 28th 2024 11:40:33 pm                                                #
# Modified   : Saturday January 25th 2025 04:40:44 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

import math

import pandas as pd

# ------------------------------------------------------------------------------------------------ #


class DataBatchGenerator:
    def __init__(self, data: pd.DataFrame, batch_size: int = 8):
        """
        Initializes the DataBatchGenerator.

        Parameters:
        - data (pd.DataFrame or list): The dataset to iterate over.
        - batch_size (int): Number of rows to return per batch.
        """
        self.data = data
        self.batch_size = batch_size
        self.index = 0
        # Calculate the number of batches
        self.n_batches = math.ceil(len(self.data) / batch_size)

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.data):
            raise StopIteration  # End of data

        # Get the next batch using iloc for DataFrame
        start_idx = self.index
        end_idx = min(start_idx + self.batch_size, len(self.data))
        batch = self.data.iloc[start_idx:end_idx]

        # Update index for the next batch
        self.index += self.batch_size

        return batch
