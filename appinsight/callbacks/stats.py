#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/callbacks/stats.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 7th 2024 12:26:18 am                                                    #
# Modified   : Friday May 31st 2024 02:53:57 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import warnings
from datetime import datetime

import numpy as np
import pandas as pd

from appinsight.callbacks.callback import Callback
from appinsight.utils.print import print_dict


# ------------------------------------------------------------------------------------------------ #
#                                 SESSION STATS CALLBACK                                           #
# ------------------------------------------------------------------------------------------------ #
class SessionStats(Callback):
    """Encapsulates the state and stats of the sentiment classification operation."""

    def __init__(self) -> None:
        super().__init__()
        self._iteration = 0
        self._classifier = None
        self._model = None
        self._dataset_size = 0
        self._start = None
        self._stop = None
        self._runtime = None
        self._train_accuracy = None
        self._val_accuracy = None
        self._overfit = None

        self._iterations = []
        self._dataset_sizes = []
        self._starts = []
        self._stops = []
        self._runtimes = []
        self._train_accuracies = []
        self._val_accuracies = []
        self._overfits = []

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def on_session_begin(self, logs: dict = None) -> None:
        super().on_session_begin()
        self._iteration = 0
        self._dataset_size = 0
        self._start = None
        self._stop = None
        self._runtime = None
        self._train_accuracy = None
        self._validation_accuracy = None
        self._test_accuracy = None
        self._overfit = None

        self._iterations = []
        self._dataset_sizes = []
        self._starts = []
        self._stops = []
        self._runtimes = []
        self._train_accuracies = []
        self._validation_accuracies = []
        self._test_accuracies = []
        self._overfits = []

    def on_session_end(self, logs: dict = None) -> None:
        d = {
            "iteration": self._iterations,
            "model": self._model.name,
            "dataset_sizes": self._dataset_sizes,
            "start": self._starts,
            "stop": self._stops,
            "runtime": self._runtimes,
            "train_accuracies": self._train_accuracies,
            "validation_accuracies": self._validation_accuracies,
            "test_accuracies": self._test_accuracies,
            "overfits": round(self._overfits, 2),
        }
        df = pd.DataFrame(data=d)
        print(df)
        super().on_session_end()

    def on_train_begin(self, logs: dict = None) -> None:
        self._iteration += 1
        self._dataset_size = logs["dataset_size"]
        self._start = datetime.now()
        self._iterations.append(self._iteration)
        self._dataset_sizes.append(logs["dataset_size"])
        self._starts.append(self._start)

    def on_train_end(self, logs: dict = None) -> None:
        self._stop = datetime.now()
        self._runtime = (self._stop - self._start).total_seconds()
        self._train_accuracy = logs["train_accuracy"]
        self._validation_accuracy = logs["validation_accuracy"]
        self._test_accuracy = logs["test_accuracy"]
        self._overfit = self._train_accuracy / self._test_accuracy

        self._stops.append(self._stop)
        self._runtimes.append(self._runtime)
        self._train_accuracies.append(self._train_accuracy)
        self._validation_accuracies.append(self._validation_accuracy)
        self._test_accuracies.append(self._test_accuracy)
        self._overfits.append(self._overfit)

        d = {
            "iteration": self._iteration,
            "model.name": self._model.name,
            "dataset_size": self._dataset_size,
            "start": self._start,
            "stop": self._stop,
            "runtime": self._runtime,
            "train_accuracy": self._train_accuracy,
            "validation_accuracy": self._validation_accuracy,
            "test_accuracy": self._test_accuracy,
            "overfit": self._overfit,
        }
        df = pd.DataFrame(d, index=[0])

        self._model.history = self._model.history

        self._logger.debug(f"{print_dict(title=self._model.name, data=d)}")
