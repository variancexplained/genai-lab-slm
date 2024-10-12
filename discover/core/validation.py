#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppVoCAI-Discover                                                                   #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /discover/core/validation.py                                                        #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appvocai-discover                               #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday October 11th 2024 07:21:01 pm                                                #
# Modified   : Friday October 11th 2024 09:33:50 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Validation Handling Module"""
# ------------------------------------------------------------------------------------------------ #
# %%
import inspect
import logging


class ValidationError(Exception):
    def __init__(self, message="Validation error occurred"):
        super().__init__(message)
        self._errors = []

    def add_error(self, error_message):
        self._errors.append(error_message)

    def log_raise(self):
        # Get the frame for the calling method
        caller_frame = inspect.stack()[1][0]

        # Initialize the module path source of validation error
        source = ""

        # module and packagename.
        module_info = inspect.getmodule(caller_frame)
        if module_info:
            module = module_info.__name__
            source += f"\nModule: {module}"

        # Check for 'self' in the calling frame to identify the class instance
        class_instance = caller_frame.f_locals.get("self", None)
        if class_instance:
            # Get the class and its module name from the instance
            class_name = class_instance.__class__.__name__
            method_name = caller_frame.f_code.co_name
            source += f"\nClass Name: {class_name}\nMethod Name: {method_name}"
        else:
            # If no 'self', assume it's a module-level function or code block
            function_name = f"{caller_frame.f_code.co_name}"
            source += f"{function_name}"

        # Join errors into a single string
        error_msg = "\n".join(self._errors)
        # Prepare a custom log message
        log_message = f"{source}\nValidation Errors:\n{error_msg}"
        # Set the message of the exception to the log_message
        self.args = (log_message,)
        # Log the message explicitly
        logging.error(log_message)
        # Clean up to avoid reference cycles
        del caller_frame
        # Raise the exception
        raise self
