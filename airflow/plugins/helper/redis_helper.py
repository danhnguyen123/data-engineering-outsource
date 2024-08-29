"""
This module helps to cache the values to Redis
"""
from logging import Logger
from redis import Redis
from helper.exceptions import RedisError

class RedisHelper:
    """
    Redis Helper to use with our service
    """

    def __init__(self, logger: Logger, redis_host, redis_port, redis_db_num=0, expire_time: int = None, health_check_interval: int = 0):
        """
        Init class
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db_num
        self.logger = logger
        self.expire_time = expire_time
        self.health_check_interval = health_check_interval

        # create resdis client for using
        self.redis_client = Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=self.redis_db, 
            decode_responses=True, 
            health_check_interval=self.health_check_interval)

    @staticmethod
    def make_cache_key(prefix, key, param_name):
        """
        Return cache key for Redis caching

        :param key: like as gpssn.
        :param param_name: param name
        :return: cache key
        """
        return f"{prefix}:{key}:{param_name}"

    def get_cached_value_for_key(self, key) -> str:
        """
        Return cache value from key input

        :param key: key for fetching value from Redis
        :return: value from key, None if value is not exist
        """
        try:
            return self.redis_client.get(key)
        except Exception as err:
            message = f"Got error when getting cached value from Redis with key:{key} and error:{err}"
            self.logger.error(message )
            raise RedisError(f"Get value failed: {message}")

    def put_cached_value_for_key(self, key, value, expire_time: int = None):
        """
        Put value to Redis for caching

        :param key: key for caching to Redis
        :param value: value for catching to Redis
        :return: result
        """
        try:
            if expire_time is None:
                expire_time = self.expire_time
            return self.redis_client.set(key, value, ex=expire_time)
        except Exception as err:
            message = f"Got error when putting value to Redis with key:{key}, value:{value} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Set value failed: {message}")

    def get_cached_value_for_key_as_dict(self, key):
        """
        Return cache value from key input as a dictionary object
        Refer link: https://stackoverflow.com/questions/32276493/how-to-store-and-retrieve-a-dictionary-with-redis
        https://redis-py.readthedocs.io/en/stable/

        :param key: key for fetching value from Redis
        :return: value from key as a dictionary object
        """
        try:
            return self.redis_client.hgetall(key)
        except Exception as err:
            message = f"Got error when getting cached value with key:{key} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Get value failed: {message}")


    def put_cached_value_for_key_as_dict(self, key, value: dict, expire_time: int = None):
        """
        Put value to Redis for caching
        Refer link: https://stackoverflow.com/questions/32276493/how-to-store-and-retrieve-a-dictionary-with-redis
        https://redis-py.readthedocs.io/en/stable/

        All key and value are need to decode if need to use if not set decode_responses=True when create redis client.
        Refer link: https://stackoverflow.com/questions/57004777/redis-py-and-hgetall-why-key-values-have-a-b/57005209

        :param key: key for caching to Redis
        :param value: value for catching to Redis
        :return: result
        """
        try:
            cache_ttl = expire_time or self.expire_time
            rs = self.redis_client.hmset(key, value)
            # set expire time for key of redis
            if cache_ttl:
                self.redis_client.expire(key, cache_ttl)
            return rs  # return result as a dict object.
        except Exception as err:
            message = f"Got error when putting value to redis with key:{key}, value:{value} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Set value failed: {message}")

    def put_cached_value_for_as_list(self, key, values: list, expire_time: int = None):
        """
        Put value to Redis for caching
        :param key: key for caching to Redis
        :param value: value for catching to Redis
        """
        try:
            cache_ttl = expire_time or self.expire_time
            for value in values:
                self.redis_client.rpush(key, value)
            if cache_ttl:
                self.redis_client.expire(key, cache_ttl)
        except Exception as err:
            message = f"Got error when putting value to redis with key:{key}, value:{value} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Set value failed: {message}")

    def get_cached_value_for_key_as_list(self, key):
        """
        Return cache value from key input
        :param key: key for fetching value from Redis
        :return: value from key as a list
        """
        try:
            len_list = self.redis_client.llen(key)
            return self.redis_client.lrange(key, 0, len_list)
        except Exception as err:
            message = f"Got error when getting cached value with key:{key} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Get value failed: {message}")

    def remove_cached_value_for_key(self, key):
        """
        Remove cache value of key input
        :param key: key for fetching value from Redis
        """
        try:
            self.redis_client.delete(key)
        except Exception as err:
            message = f"Got error when remove cached value with key:{key} and error:{err}"
            self.logger.error(message)
            raise RedisError(f"Remove key failed: {message}")
