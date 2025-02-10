#!/bin/bash
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /scripts/java/set_java_home.sh                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 29th 2025 01:05:41 pm                                             #
# Modified   : Saturday February 8th 2025 10:43:06 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
#!/bin/bash

# Set JAVA_HOME in the terminal session
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Add to bashrc (if not already present)
if ! grep -q "JAVA_HOME" ~/.bashrc; then
    echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> ~/.bashrc
fi
if ! grep -q "PATH=.*\$JAVA_HOME/bin" ~/.bashrc; then
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
fi

# Confirm it's set
echo "JAVA_HOME is set to: $JAVA_HOME"

# Update bashrc
source ~/.bashrc  # Apply changes

# Verify Settings
echo "JAVA_HOME is now set to: $JAVA_HOME"  # Should print /usr/lib/jvm/java-8-openjdk-amd64
java -version    # Should show Java 8
