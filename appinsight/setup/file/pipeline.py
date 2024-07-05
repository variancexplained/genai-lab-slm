#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/setup/file/pipeline.py                                                  #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 09:21:57 pm                                                  #
# Modified   : Friday July 5th 2024 12:17:25 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from prefect import Flow, flow

from appinsight.setup.file.config import FileSetupPipelineConfig
from appinsight.setup.file.download import DownloadFileTask
from appinsight.setup.file.extract import ExtractFileTask
from appinsight.setup.file.sample import SampleFileTask


# ------------------------------------------------------------------------------------------------ #
def create_file_setup_pipeline(config: FileSetupPipelineConfig) -> Flow:

    # ------------------------------------------------------------------------------------------------ #
    # Instantiate the tasks
    download_task = DownloadFileTask(
        aws_access_key=config.aws_access_key,
        aws_secret_access_key=config.aws_secret_access_key,
        aws_region_name=config.aws_region_name,
        aws_bucket_name=config.aws_bucket_name,
        aws_folder=config.aws_folder,
        aws_s3_key=config.aws_s3_key,
        local_download_folder=config.local_download_folder,
        local_download_filepath=config.local_download_filepath,
        force=config.force,
    )
    # ------------------------------------------------------------------------------------------------ #
    extract_task = ExtractFileTask(
        source=config.local_download_filepath,
        destination=config.extract_destination,
        force=config.force,
    )
    # ------------------------------------------------------------------------------------------------ #
    sample_task = SampleFileTask(
        source=config.extract_destination,
        destination=config.sample_destination,
        frac=config.frac,
        force=config.force,
        **config.save_kwargs,
    )

    # ------------------------------------------------------------------------------------------------ #
    # Define Prefect flow
    @flow
    def file_setup_pipeline():
        # Define task dependencies
        download_task()
        extract_task()
        sample_task()

    return flow
