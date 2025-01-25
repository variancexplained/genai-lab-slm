#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /genailabslm/infra/utils/file/tempfile.py                                           #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 2nd 2024 12:19:03 pm                                                    #
# Modified   : Saturday January 25th 2025 04:44:56 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
import os
import shutil

from dotenv import load_dotenv


# ------------------------------------------------------------------------------------------------ #
class TempFileMgr:
    def __init__(self, tempdir: str = None):
        load_dotenv()
        self.tempdir = tempdir or os.getenv("TEMPDIR")
        os.makedirs(self.tempdir, exist_ok=True)
        self.files = []

    def add_temp_file(self, filename: str):
        filepath = os.path.join(self.tempdir, filename)
        self.files.append(filepath)
        return filepath

    def cleanup(self):
        for filepath in self.files:
            if os.path.exists(filepath):
                if os.path.isdir(filepath):
                    shutil.rmtree(filepath)
                else:
                    os.remove(filepath)
        if os.path.exists(self.tempdir) and not os.listdir(self.tempdir):
            shutil.rmtree(self.tempdir)
        self.files = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()
