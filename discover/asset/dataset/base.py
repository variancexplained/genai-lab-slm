#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/asset/dataset/base.py                                                     #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday December 27th 2024 09:21:00 pm                                               #
# Modified   : Saturday December 28th 2024 12:30:28 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Dataset Base Module"""

from pydantic.dataclasses import dataclass

from discover.asset.base import AssetComponent, AssetComponentBuilder


# ------------------------------------------------------------------------------------------------ #
#                               DATASET COMPONENT                                                  #
# ------------------------------------------------------------------------------------------------ #
@dataclass
class DatasetComponent(AssetComponent):
    pass


# ------------------------------------------------------------------------------------------------ #
#                            DATASET COMPONENT BUILDER                                             #
# ------------------------------------------------------------------------------------------------ #
class DatasetComponentBuilder(AssetComponentBuilder):
    pass
