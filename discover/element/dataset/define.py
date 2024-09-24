#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/element/dataset/define.py                                                 #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 01:35:04 am                                              #
# Modified   : Monday September 23rd 2024 07:27:16 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #

from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pyspark

from discover.element.base.define import Element
from discover.element.dataset.store import (
    PandasParquetDatasetStorageConfig,
    PandasParquetPartitionedDatasetStorageConfig,
    SparkParquetDatasetStorageConfig,
    SparkParquetPartitionedDatasetStorageConfig,
)


# ------------------------------------------------------------------------------------------------ #
@dataclass
class Dataset(Element):
    storage_config: Optional[PandasParquetDatasetStorageConfig] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasDataset(Dataset):
    content: Optional[pd.DataFrame] = None
    storage_config: Optional[PandasParquetDatasetStorageConfig] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0


# ------------------------------------------------------------------------------------------------ #
@dataclass
class PandasPartitionedDataset(Dataset):
    content: Optional[pd.DataFrame] = None
    storage_config: Optional[PandasParquetPartitionedDatasetStorageConfig] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkDataset(Dataset):
    content: Optional[pyspark.sql.DataFrame] = None
    storage_config: Optional[SparkParquetDatasetStorageConfig] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0


# ------------------------------------------------------------------------------------------------ #
@dataclass
class SparkPartitionedDataset(Dataset):
    content: Optional[pyspark.sql.DataFrame] = None
    storage_config: Optional[SparkParquetPartitionedDatasetStorageConfig] = None
    nrows: int = 0
    ncols: int = 0
    size: float = 0
