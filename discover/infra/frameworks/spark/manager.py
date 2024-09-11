#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/frameworks/spark/manager.py                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday June 3rd 2024 02:27:56 am                                                    #
# Modified   : Tuesday September 10th 2024 08:46:24 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Spark Session Manager Task"""
from pyspark.sql import SparkSession

from discover.infra.frameworks.spark.factory import SparkSessionFactory


# ------------------------------------------------------------------------------------------------ #
class SparkSessionManager:
    """
    Manages the lifecycle of a Spark session with context management support.

    Attributes:
        session_factory_cls (type): The class used to create the Spark session factory.
        spark (SparkSession): The current Spark session.
    """

    def __init__(
        self, session_factory_cls: type[SparkSessionFactory] = SparkSessionFactory
    ):
        """
        Initializes the SparkSessionManager with the given session factory class.

        Args:
            session_factory_cls (type, optional): The class to create the Spark session factory.
                                                  Defaults to SparkSessionFactory.
        """
        self.session_factory_cls = session_factory_cls
        self.spark = None

    def build(self, nlp: bool = False) -> SparkSession:
        """
        Builds a Spark session configured for NLP if specified.

        Args:
            nlp (bool, optional): If True, configures the session for NLP. Defaults to False.

        Returns:
            SparkSession: The configured Spark session.
        """
        self.spark = self.session_factory_cls().build(nlp=nlp)
        return self.spark

    def __enter__(self) -> SparkSession:
        """
        Enters the runtime context related to this object.

        If a Spark session has not been built yet, it builds one.

        Returns:
            SparkSession: The current Spark session.
        """
        if self.spark is None:
            self.build()
        return self.spark

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """
        Exits the runtime context related to this object.

        Stops the Spark session if it is currently active.

        Args:
            exc_type (type): The exception type.
            exc_value (Exception): The exception instance.
            traceback (traceback): The traceback object.
        """
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
