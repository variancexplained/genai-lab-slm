#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab-SLM                                                                       #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /genailab/infra/utils/data/object.py                                                #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Friday September 20th 2024 07:10:39 pm                                              #
# Modified   : Sunday January 26th 2025 10:41:24 pm                                                #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #


def get_object_name(obj) -> str:
    """
    Returns the class name if the object has one, otherwise the qualified name (qualname) if applicable,
    and finally the type name if neither a class nor qualname makes sense.

    Parameters:
    -----------
    obj : Any
        The object to inspect, which can be a class, method, function, or any other type.

    Returns:
    --------
    str
        The class name, qualname, or type of the object depending on its type.

    Examples:
    ---------
    >>> class ExampleClass:
    >>>     def method(self):
    >>>         pass

    >>> get_class_or_qualname(ExampleClass)
    'ExampleClass'

    >>> get_class_or_qualname(ExampleClass.method)
    'ExampleClass.method'

    >>> get_class_or_qualname(42)
    'int'
    """
    # Check if the object is a class
    if isinstance(obj, type):
        return obj.__name__

    # Check if the object is an instance of a class
    elif hasattr(obj, "__class__"):
        return obj.__class__.__name__

    # Check if the object has a __qualname__ attribute (functions, methods)
    elif hasattr(obj, "__qualname__"):
        return obj.__qualname__

    # Return the type name if neither class name nor qualname is available
    else:
        return type(obj).__name__
