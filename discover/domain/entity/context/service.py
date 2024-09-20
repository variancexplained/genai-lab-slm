#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/entity/context/service.py                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 02:12:54 pm                                              #
# Modified   : Thursday September 19th 2024 09:04:21 pm                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Context entity module"""

from dataclasses import dataclass

from discover.domain.entity.context.base import Context


# ------------------------------------------------------------------------------------------------ #
@dataclass
class ServiceContext(Context):
    """
    Represents a context for a service-level process, inheriting from the base `Context`.
    This class currently does not add any additional attributes but serves as a placeholder for
    future service-specific context details.
    """

    pass
