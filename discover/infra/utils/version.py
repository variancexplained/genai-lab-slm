#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /discover/infra/utils/version.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Monday May 6th 2024 09:11:54 pm                                                     #
# Modified   : Tuesday September 10th 2024 07:36:59 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import shelve

from dotenv import load_dotenv

load_dotenv()
# ------------------------------------------------------------------------------------------------ #


class Version:
    def __init__(self) -> None:
        self._filepath = os.getenv("VERSION_FILEPATH")
        os.makedirs(name=os.path.dirname(self._filepath), exist_ok=True)

    def reset(self, entity: str) -> None:
        with shelve.open(self._filepath) as db:
            db[entity] = 1

    def get_version(self, entity: str) -> str:
        with shelve.open(self._filepath) as db:
            db[entity] += 1
            return f"v.{db[entity]}"
