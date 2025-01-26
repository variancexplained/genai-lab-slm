#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailabslm/__main__.py                                                            #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Saturday September 14th 2024 06:28:52 am                                            #
# Modified   : Sunday January 26th 2025 06:34:10 am                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import argparse
import logging

from dotenv import load_dotenv
from genailabslm.setup import load_data, wire_container

# ------------------------------------------------------------------------------------------------ #
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------------------------ #
def main(force: bool = False):
    container = wire_container()
    load_data(container=container, force=force)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Description of your script.")
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",  # This makes it a boolean flag
        help="Force certain actions.",
    )

    args = parser.parse_args()
    load_dotenv(dotenv_path=".env", override=True)
    main(force=args.force)
