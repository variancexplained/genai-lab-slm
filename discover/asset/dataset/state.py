#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/state.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 22nd 2025 01:20:36 am                                             #
# Modified   : Friday January 24th 2025 08:28:23 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
"""Dataset State Module"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

import pandas as pd

from discover.core.dstruct import DataClass


# ------------------------------------------------------------------------------------------------ #
class DatasetStateDef(Enum):
    CREATED = ("created", "Dataset Created")
    PUBLISHED = ("published", "Dataset Published to Repository")
    CONSUMED = ("consumed", "Dataset Consumed")

    @classmethod
    def from_value(cls, value) -> DatasetStateDef:
        """Finds the enum member based on a given value"""
        for member in cls:
            if member.value == value:
                return member
        raise ValueError(f"No matching {cls.__name__} for value: {value}")

    def __new__(cls, name: str, label: str):
        obj = object.__new__(cls)
        obj._value_ = name
        obj.label = label
        return obj


# ------------------------------------------------------------------------------------------------ #


class DatasetState(DataClass):
    """
    A class representing the state and lifecycle of a dataset. The class tracks
    the status, creation time, last accessed time, and events related to the dataset.

    Attributes:
        status (DatasetStateDef): The current status of the dataset.
        created (datetime): The timestamp when the dataset was created.
        published (datetime): The timestamp when the dataset was published.
        consumed (datetime): The timestamp when the dataset was consumed.
        accessed (datetime): The timestamp when the dataset was last accessed.
        modified (datetime): The timestamp when the dataset was last modified.
        eventlog (list): A log of events related to the dataset.

    Methods:
        publish(entity: str) -> None:
            Marks the dataset as published and logs the event.
        consume(entity: str) -> None:
            Marks the dataset as consumed and logs the event.
        access(entity: str) -> None:
            Records the timestamp of when the dataset was accessed and logs the event.
        add_event(entity: str, event: str) -> None:
            Adds an event to the event log.
        get_events() -> pd.DataFrame:
            Returns a DataFrame of the event log.
    """

    def __init__(self, creator: str) -> None:
        """
        Initializes a new DatasetState instance with the creator's name and sets
        the initial values for the dataset's attributes.

        Args:
            creator (str): The entity responsible for creating the dataset.
        """
        self._creator = creator
        self._status = None
        self._created = None
        self._published = None
        self._consumed = None
        self._accessed = None
        self._modified = None
        self._eventlog = []

        self._initialize()

    @property
    def status(self) -> DatasetStateDef:
        """
        Returns the current status of the dataset.

        Returns:
            DatasetStateDef: The current status of the dataset.
        """
        return self._status

    @property
    def created(self) -> datetime:
        """
        Returns the timestamp of when the dataset was created.

        Returns:
            datetime: The creation timestamp of the dataset.
        """
        return self._created

    @property
    def accessed(self) -> Optional[datetime]:
        """
        Returns the timestamp of when the dataset was last accessed.

        Returns:
            datetime: The last accessed timestamp of the dataset.
        """
        return self._accessed

    @property
    def published(self) -> Optional[datetime]:
        """
        Returns the timestamp of when the dataset was published.

        Returns:
            datetime: The datetime the dataset was published.
        """
        return self._published

    @property
    def consumed(self) -> Optional[datetime]:
        """
        Returns the timestamp of when the dataset was consumed.

        Returns:
            datetime: The datetime the dataset was consumed.
        """
        return self._consumed

    @property
    def modified(self) -> datetime:
        """
        Returns the timestamp of when the dataset was last modified.

        Returns:
            datetime: The last modified timestamp of the dataset.
        """
        return self._modified

    def publish(self, entity: str) -> None:
        """
        Marks the dataset as published and logs the event.

        Args:
            entity (str): The entity that publishes the dataset.
        """
        self._status = DatasetStateDef.PUBLISHED
        self._modified = datetime.now()
        event = f"{self._status.label} by {entity}"
        self.add_event(entity=entity, event=event)

    def consume(self, entity: str) -> None:
        """
        Marks the dataset as consumed and logs the event.

        Args:
            entity (str): The entity that consumes the dataset.
        """
        self._status = DatasetStateDef.CONSUMED
        self._modified = datetime.now()
        event = f"{self._status.label} by {entity}"
        self.add_event(entity=entity, event=event)

    def access(self, entity: str) -> None:
        """
        Records the timestamp of when the dataset was accessed and logs the event.

        Args:
            entity (str): The entity that accessed the dataset.
        """
        self._accessed = datetime.now()
        event = f"Accessed by {entity}"
        self.add_event(entity=entity, event=event)

    def add_event(self, entity: str, event: str) -> None:
        """
        Adds an event to the event log with a timestamp.

        Args:
            entity (str): The entity responsible for the event.
            event (str): A description of the event.
        """
        event_entry = {
            "timestamp": datetime.now(),
            "entity": entity,
            "event": event,
        }
        self._eventlog.append(event_entry)
        self._modified = datetime.now()

    def get_events(self) -> pd.DataFrame:
        """
        Returns the event log as a pandas DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing the event log with timestamps,
            entities, and event descriptions.
        """
        return pd.DataFrame(self._eventlog)

    def _initialize(self) -> None:
        """
        Initializes the dataset's creation timestamp and sets the status to 'created'.
        Logs the initial event of the dataset creation.
        """
        self._created = datetime.now()
        self._status = DatasetStateDef.CREATED
        event = f"{self._status.label} by {self._creator}"
        self.add_event(entity=self._creator, event=event)
