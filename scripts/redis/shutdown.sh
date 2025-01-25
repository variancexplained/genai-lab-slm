#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /scripts/redis/shutdown.sh                                                          #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday September 25th 2024 02:10:29 pm                                           #
# Modified   : Saturday January 25th 2025 04:43:26 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024-2025 John James                                                            #
# ================================================================================================ #
sudo systemctl stop redis-server
sudo systemctl stop redis-dev
sudo systemctl stop redis-test
sudo systemctl stop redis-prod
