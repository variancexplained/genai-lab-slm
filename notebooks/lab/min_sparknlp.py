#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : GenAI-Lab                                                                           #
# Version    : 0.1.0                                                                               #
# Python     : 3.10.14                                                                             #
# Filename   : /notebooks/lab/min_sparknlp.py                                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/genai-lab-slm                                   #
# ------------------------------------------------------------------------------------------------ #
# Created    : Wednesday January 29th 2025 03:03:32 pm                                             #
# Modified   : Saturday February 8th 2025 10:43:31 pm                                              #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2025 John James                                                                 #
# ================================================================================================ #
import os

from pyspark.sql import SparkSession
from sparknlp.base import DocumentAssembler  # Just one annotator

print("SPARK_HOME:", os.environ.get("SPARK_HOME"))
print("JAVA_HOME:", os.environ.get("JAVA_HOME"))
print("PYTHONPATH:", os.environ.get("PYTHONPATH"))

try:
    spark = (
        SparkSession.builder.appName("MinimalSparkNLP")
        .master("local[*]")
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.5.2")
        .getOrCreate()
    )

    assembler = DocumentAssembler().setInputCol("text").setOutputCol("document") # Only DocumentAssembler

    data = spark.createDataFrame([("This is a test.",)], ["text"])
    result = assembler.transform(data) # Directly use the assembler
    result.show()

    spark.stop()

except Exception as e:
    print("Error:", e)
    import traceback
    traceback.print_exc()