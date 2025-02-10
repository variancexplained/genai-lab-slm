#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /scripts/redis/shutdown.sh                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 25th 2024 02:10:29 pm                                           #
# Modified   : Saturday February 8th 2025 10:43:06 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024-2025 John James                                                            #
# ================================================================================================ #
sudo systemctl stop redis-server
sudo systemctl stop redis-dev
sudo systemctl stop redis-test
sudo systemctl stop redis-prod
