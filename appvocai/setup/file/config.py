#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/setup/file/config.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 09:05:51 pm                                                  #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""File Setup Pipeline Config Module"""
import os
from collections import defaultdict
from dataclasses import dataclass, field

from appvocai import DataClass
from appvocai.shared.config.config import Config


# ------------------------------------------------------------------------------------------------ #
@dataclass
class FileSetupPipelineConfig(DataClass):
    aws_access_key: str = None
    aws_secret_access_key: str = None
    aws_region_name: str = None
    aws_bucket_name: str = None
    aws_folder: str = None
    aws_s3_key: str = None
    local_download_folder: str = None
    local_download_filepath: str = None
    extract_destination: str = None
    sample_destination: str = None
    frac: float = None
    save_kwargs: defaultdict[dict] = field(default_factory=lambda: defaultdict(dict))
    force: bool = False

    def __post_init__(self) -> None:
        config = Config()
        self.aws_access_key = config.aws.access_key
        self.aws_secret_access_key = config.aws.secret_access_key
        self.aws_region_name = config.aws.region
        self.aws_bucket_name = config.aws_file.bucket_name
        self.aws_folder = config.aws_file.folder
        self.aws_s3_key = config.aws_file.s3_key
        self.local_download_folder = "data/ext"
        self.local_download_filepath = os.path.join(
            self.local_download_folder, self.aws_s3_key
        )
        self.extract_destination = "data/ext/reviews"
        self.sample_destination = "00_raw/reviews"
        self.frac = config.dataset["frac"]
        self.save_kwargs = config.dataset["save_kwargs"]

        self.force = False
