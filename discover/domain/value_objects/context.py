#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/domain/value_objects/context.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 13th 2024 02:12:54 pm                                              #
# Modified   : Saturday September 14th 2024 04:38:45 am                                            #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from typing import Optional, Union

from discover.core.data import DataClass
from discover.domain.service.base.identity import IDXGen
from discover.domain.value_objects.lifecycle import Stage


# ------------------------------------------------------------------------------------------------ #
class Context(DataClass):
    def __init__(self, idxgen_cls: IDXGen) -> None:
        self._idxgen = idxgen_cls()
        self.service_type: Optional[str] = None
        self.service_name: Optional[str] = None
        self.stage: Optional[Stage] = None
        self.runid: Optional[str] = None

    def create_run(self, owner: Union[type, object]) -> None:
        self.runid = self._idxgen.get_next_id(owner=owner)
