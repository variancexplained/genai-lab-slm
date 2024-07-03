#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/__main__.py                                                             #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday May 25th 2024 03:48:28 am                                                  #
# Modified   : Tuesday July 2nd 2024 10:21:15 pm                                                   #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Main Module"""
import argparse
import os
import shutil
import signal
import sys
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from appinsight.infrastructure.dependency.container import AppInsightContainer
from appinsight.infrastructure.persist.file.io import IOService
from appinsight.utils.datetime import convert_seconds_to_hms
from appinsight.utils.repo import ReviewRepo

# ------------------------------------------------------------------------------------------------ #
filepath = "data/reviews.pkl"
# ------------------------------------------------------------------------------------------------ #


def load_data() -> pd.DataFrame:
    """Loads the review dataseet."""
    print("Loading dataset.")
    start = datetime.now()
    df = IOService.read(filepath=filepath)
    stop = datetime.now()
    duration = convert_seconds_to_hms((stop - start).total_seconds())
    print(f"Dataset loaded in {duration}.")
    return df


# ------------------------------------------------------------------------------------------------ #
def setup_application(container: AppInsightContainer, df: pd.DataFrame):
    """
    Set up the application by creating database tables.

    This function creates database tables required by the application using TableSetup class.
    """
    # Setup datasets for the environment
    dsa = container.dataset.setup()
    dsa.execute(data=df)

    # Create Database


# ------------------------------------------------------------------------------------------------ #
def build_datasets(force, container) -> None:
    """Constructs the raw and normalized datasets"""
    repo = ReviewRepo()
    if force or not repo.exists(directory="00_raw", filename="reviews.pkl"):
        filepath = repo.get_filepath(directory="00_raw", filename="reviews.pkl")
        try:
            shutil(os.path.dirname(filepath))
        except Exception:
            pass
        df = load_data()
        # Build raw dataset
        setup = container.dataset.setup()
        dataset = setup.execute(data=df)
        ReviewRepo().write(directory="00_raw", filename="reviews.pkl", data=dataset)


# ------------------------------------------------------------------------------------------------ #
def build_database(container) -> None:
    """Constructs the database tables."""
    dba = container.db.admin()
    dba.create_table(tablename="profile")
    print("Database is setup successfully.")


# ------------------------------------------------------------------------------------------------ #
def build_dependencies(env: str):
    """Initializes the application dependencies."""
    # Initialize application container
    container = AppInsightContainer()
    container.init_resources()
    container.wire(
        packages=[
            "appinsight.infrastructure.persist.database",
            "appinsight.infrastructure.profiling",
        ],
        modules=["appinsight.infrastructure.persist.repo"],
    )  # Wire the current module
    print(f"Dependencies are initialized for the {env} environment.")

    # Setup signal handlers for graceful shutdown
    def shutdown_handler(signum, frame):
        print(f"Received signal {signum}. Shutting down gracefully...")
        container.shutdown_resources()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    return container


# ------------------------------------------------------------------------------------------------ #

# TODO: Figure out setup vs reset


def main():
    """Main function for running the AppInsight application.

    This function parses command-line arguments, initializes the application container, sets up signal handlers
    for graceful shutdown, and runs the application if specified for each environment.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="AppInsight Application")
    parser.add_argument("-f", "--force", action="store_true", help="Force build")
    args = parser.parse_args()
    force = True if args.force else False

    # Start the clock
    start = datetime.now()
    # load the env variable
    load_dotenv()
    env = os.getenv("ENV")
    # Build the dependencies for the environment.
    container = build_dependencies(env=env)
    # Build database
    if force:
        build_database(container=container)
        # Build Datasets
        build_datasets(force=force, container=container)
    # Capture the time
    stop = datetime.now()
    duration = convert_seconds_to_hms((stop - start).total_seconds())
    # Announce conclusion
    print(f"AppInsight application is configured in {duration}.")


# ------------------------------------------------------------------------------------------------ #
# Entry point
if __name__ == "__main__":
    main()
