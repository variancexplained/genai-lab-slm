#!/bin/bash
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /scripts/redis/uninstall.sh                                                         #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday September 22nd 2024 02:50:05 pm                                              #
# Modified   : Saturday January 25th 2025 04:44:32 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024-2025 John James                                                            #
# ================================================================================================ #

# Stop the Redis service if it's running
echo "Stopping Redis service..."
sudo systemctl stop redis
sudo systemctl stop redis-server

# Uninstall Redis and remove related packages
echo "Uninstalling Redis..."
sudo apt-get purge --auto-remove -y redis-server

# Remove Redis configuration files
echo "Removing Redis configuration files..."
sudo rm -rf /etc/redis/

# Remove Redis data directory
echo "Removing Redis data directory..."
sudo rm -rf /var/lib/redis/

# Remove Redis log directory
echo "Removing Redis log files..."
sudo rm -rf /var/log/redis/

# Remove Redis user and group
echo "Removing Redis user and group..."
sudo deluser --remove-home redis
sudo deluser redis
sudo delgroup redis

# Update the packge database
echo "Updating the package database..."
sudo apt-get update

# Confirmation message
echo "Redis uninstalled and all remnants removed!"
