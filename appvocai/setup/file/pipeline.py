#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appvocai/setup/file/pipeline.py                                                    #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Thursday July 4th 2024 09:21:57 pm                                                  #
# Modified   : Tuesday August 27th 2024 10:54:14 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
from appvocai.setup.file.config import FileSetupPipelineConfig
from appvocai.setup.file.download import DownloadFileTask
from appvocai.setup.file.extract import ExtractFileTask
from appvocai.setup.file.sample import SampleFileTask
from prefect import Flow, flow


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
