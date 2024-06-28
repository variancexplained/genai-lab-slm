#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvoc/callbacks/early_stop.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Tuesday May 7th 2024 12:44:30 am                                                    #
# Modified   : Friday May 31st 2024 02:52:50 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import logging
import warnings
from datetime import datetime

import numpy as np
from appinsight.callbacks.callback import Callback


# ------------------------------------------------------------------------------------------------ #
#                                     EARLY STOP CALLBACK                                          #
# ------------------------------------------------------------------------------------------------ #
class EarlyStop(Callback):

    def __init__(
        self,
        monitor: str = "test_accuracy",
        min_delta: float = 0,
        patience: int = 3,
        mode: str = "auto",
        restore_best_model: bool = True,
    ) -> None:
        super().__init__()
        self._monitor = monitor
        self._min_delta = min_delta
        self._patience = patience
        self._model = None
        self._restore_best_model = restore_best_model

        self._wait = 0
        self._iteration = 0
        self._stopped_iteration = 0
        self._best_model = None
        self._best_iteration = 0
        self._start = None
        self._stop = None
        self._stopped_early = False

        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        if mode not in ["auto", "min", "max"]:
            warnings.warn(
                f"EarlyStopping mode {mode} is unknown, fallback to auto mode.",
                stacklevel=2,
            )
            mode = "auto"
        self._mode = mode
        self._monitor_op = self._set_monitor_op()

    def _set_monitor_op(self):
        if self._mode == "min":
            self._monitor_op = np.less
        elif self._mode == "max":
            self._monitor_op = np.greater
        else:
            metric_name = self._monitor
            if "loss" in metric_name:
                self._monitor_op = np.less
            elif "accuracy" in metric_name:
                self._monitor_op = np.greater
        if self._monitor_op is None:
            raise ValueError(
                f"EarlyStopping callback received monitor={self._monitor} "
                "but AppInsight isn't able to automatically determine whether "
                "that metric should be maximized or minimized. "
                "Pass `mode='max'` in order to do early stopping based "
                "on the highest metric value, or pass `mode='min'` "
                "in order to use the lowest value."
            )
        if self._monitor_op == np.less:
            self._min_delta *= -1

        self._best = float("inf") if self._monitor_op == np.less else -float("inf")

    def on_session_begin(self, logs: dict = None) -> None:
        super().on_session_begin()
        self._wait = 0
        self._iteration = 0
        self._stopped_iteration = 0
        self._best_model = None
        self._best_iteration = 0
        self._best = float("inf") if self._monitor_op == np.less else -float("inf")
        self._start = datetime.now()

    def on_session_end(self, logs: dict = None) -> None:

        self._stop = datetime.now()
        super().on_session_end()

    def on_train_end(self, logs: dict = None) -> None:
        self._iteration += 1

        if self._monitor_op is None:
            self._set_monitor_op()

        current = self.get_monitor_value(logs)
        if current is None:
            # If no monitor value exists or still in initial warm-up stage.
            return

        if self._restore_best_model and self._best_model is None:
            # If best model is never set,
            # then the current model is the best.
            self._best_model = self._model.model
            self._best_iteration = self._iteration

        if self._is_improvement(current, self._best):
            if self._best != float("inf") and self._best != float("-inf"):
                self._announce_improvement(current, self._best)
            self._best = current
            self._best_iteration = self._iteration
            if self._restore_best_model:
                self._best_model = self._model.model
            self._wait = 0
            return
        else:
            self._wait += 1

        if self._wait >= self._patience and self._iteration > 0:
            # Patience has been exceeded: stop training
            self._stopped_early = True
            self._stopped_iteration = self._iteration
            self._announce_early_stop()
            self._model.stop_session = True
            return

    def get_monitor_value(self, logs):
        logs = logs or {}
        monitor_value = logs.get(self._monitor)
        if monitor_value is None:
            warnings.warn(
                (
                    f"Early stopping conditioned on metric `{self._monitor}` "
                    "which is not available. "
                    f"Available metrics are: {','.join(list(logs.keys()))}"
                ),
                stacklevel=2,
            )
        return monitor_value

    def _is_improvement(self, monitor_value, reference_value):
        return self._monitor_op(monitor_value - self._min_delta, reference_value)

    def _announce_improvement(self, current: float, best: float) -> None:
        improvement = abs(round((current - best) / best * 100, 2))
        if self._monitor_op == np.less:
            msg = (
                f"\n{self._monitor} improved by {improvement}% from {best} to {current}"
            )
        else:
            msg = (
                f"\n{self._monitor} improved by {improvement}% from {current} to {best}"
            )
        self._logger.info(msg)

    def _announce_early_stop(self) -> None:
        msg = f"\nEarly stopping at iteration {self._stopped_iteration} with {self._monitor} = {self._best}."
        self._logger.info(msg)

    def _announce_stop(self) -> None:
        msg = f"\nStopping at iteration {self._stopped_iteration} with {self._monitor} = {self._best}."
        self._logger.info(msg)
