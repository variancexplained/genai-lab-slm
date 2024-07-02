#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /chgenv.py                                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday July 1st 2024 11:16:04 pm                                                    #
# Modified   : Monday July 1st 2024 11:18:27 pm                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
# Script for changing the current environment in the .env file.

import sys

from appinsight.infrastructure.utils.env import EnvManager


# ------------------------------------------------------------------------------------------------ #
def main():
    if len(sys.argv) != 2:
        print("Usage: python -m chgenv 'value'")
        return

    env_value = sys.argv[1]
    env_manager = EnvManager()  # Initialize your EnvManager class

    try:
        env_manager.change_environment(new_value=env_value)
    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
