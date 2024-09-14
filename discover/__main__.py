#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/__main__.py                                                               #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 06:28:52 am                                            #
# Modified   : Saturday September 14th 2024 04:13:08 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import argparse
import logging

from dependency_injector.wiring import Provide, inject

from discover.container import DiscoverContainer
from discover.infra.config.reader import ConfigReader
from discover.infra.database.schema import schema
from discover.infra.database.sqlite import SQLiteDBA

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
@inject
def setup_database(
    dba: SQLiteDBA = Provide[DiscoverContainer.db.admin],
    config_reader_cls: type[ConfigReader] = ConfigReader,
    force: bool = False,
) -> None:
    config_reader = config_reader_cls()
    env = config_reader.get_environment()
    if force:
        dba.drop_table(tablename="profile")
        logger.info(f"Profile table dropped from the {env} environment.")
        dba.create_table(schema=schema["profile"])
        logger.info(f"Profile table created in the {env} environment.")
    elif dba.exists(tablename="profile"):
        logger.info(f"The profile table already exists in the {env} environment.")
    else:
        dba.create_table(schema=schema["profile"])
        logger.info(f"Profile table created in the {env} environment.")


# ------------------------------------------------------------------------------------------------ #
def wire_container():
    container = DiscoverContainer()
    container.init_resources()
    container.wire(modules=[__name__])


# ------------------------------------------------------------------------------------------------ #
def main():
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Database setup utility")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force the recreation of the 'profile' table",
    )

    # Parse the arguments
    args = parser.parse_args()

    # Wire the container
    wire_container()

    # Call the setup_database function with the appropriate force flag
    setup_database(force=args.force)


if __name__ == "__main__":
    main()
