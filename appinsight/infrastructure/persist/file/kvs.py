#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : AppInsight                                                                          #
# Version    : 0.1.0                                                                               #
# Python     : 3.12.3                                                                              #
# Filename   : /appinsight/infrastructure/persist/file/kvs.py                                      #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john@variancexplained.com                                                           #
# URL        : https://github.com/variancexplained/appinsight                                      #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday June 30th 2024 10:27:18 pm                                                   #
# Modified   : Monday July 1st 2024 05:22:02 am                                                    #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2024 John James                                                                 #
# ================================================================================================ #
"""Key Value Store Module"""
import pickle

import redis

from appinsight.domain.base import Entity


# ------------------------------------------------------------------------------------------------ #
class KVS:
    """Key Value Store via Redis"""

    def create(self, obj: Entity) -> None:
        """Persists the object in the KVS

        Args:
            obj (Entity): an Entity object with an oid

        """
        # Serialize the object using pickle
        serialized_obj = pickle.dumps(obj)

        # Connect to Redis (assuming Redis server is running locally)
        redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

        # Set the serialized object in Redis with a key
        redis_client.set(str(obj.oid), serialized_obj)

        print(f"Object oid {obj.oid}: {obj.__class__.__name__} stored in Redis.")

    def read(self, oid: int) -> Entity:
        """Reads an object from KV store.

        Args:
            oid (int): Object id.
        """
        # Connect to Redis (assuming Redis server is running locally)
        redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

        # Retrieve the serialized object from Redis
        serialized_obj = redis_client.get(str(oid))

        # Deserialize
        if serialized_obj:
            retrieved_obj = pickle.loads(serialized_obj)
            print("Object retrieved from redis")  # Output: Hello, Alice!
            return retrieved_obj
        else:
            print("Object not found in Redis.")

    def delete(self, oid: int) -> None:
        """Delete an object from KV store.

        Args:
            oid (int): Object id.
        """
        # Connect to Redis (assuming Redis server is running locally)
        redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

        # Delete the object from Redis
        result = redis_client.delete(str(oid))

        if result == 1:
            print(f"Object oid {oid} deleted from Redis.")
        else:
            print(f"Object oid {oid} not found in Redis.")


# Example usage
if __name__ == "__main__":
    kvs = KVS()

    # Create an entity and store it
    entity = Entity(oid=1)
    kvs.create(entity)

    # Read the entity
    retrieved_entity = kvs.read(1)

    # Delete the entity
    kvs.delete(1)
