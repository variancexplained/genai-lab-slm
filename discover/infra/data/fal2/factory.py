#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fal2/factory.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday December 19th 2024 06:51:27 am                                             #
# Modified   : Thursday December 19th 2024 01:40:47 pm                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/infra/data/fal/base.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 05:39:55 pm                                              #
# Modified   : Thursday December 19th 2024 06:22:44 am                                             #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Access Layer Base Module"""

from typing import Union

import pandas as pd
import pyspark

from discover.core.data_structure import DataFrameType
from discover.infra.data.base import DataFrameReader, DataFrameWriter

# ------------------------------------------------------------------------------------------------ #
DataFrame = Union[pd.DataFrame, pyspark.sql.DataFrame]


# ------------------------------------------------------------------------------------------------ #
class FAOFactory:
    """File Access Object Factory"""

    def get_fao(self,  dataframe_type: DataFrameType, **kwargs) -> FAO:

    def get_reader(self, dataframe_type: DataFrameType, **kwargs) -> DataFrameReader:
        """Returns a DataReader for the DataFrame type.
        Args:
            dataframe_type(DataFrameType): Type of DataFrame object to be read.
            **kwargs: Arbitrary keyword arguments.
        """

    def get_writer(self, dataframe_type: DataFrameType, **kwargs) -> DataFrameWriter:
        """Returns a DataWriter for the DataFrame type.
        Args:
            dataframe_type(DataFrameType): Type of DataFrame object to be written.
            **kwargs: Arbitrary keyword arguments.
        """
