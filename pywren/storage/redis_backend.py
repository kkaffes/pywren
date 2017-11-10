import redis

from .exceptions import StorageNoSuchKeyError


class RedisBackend(object):
    """
    A wrap-up around Redis API
    """

    def __init__(self, redisconfig):
        self.client = redis.Redis(host=redisconfig['endpoint'],
                                  port=int(redisconfig['port']))

    def put_object(self, key, data):
        """
        Put an object in Redis. Override the object if the key already exists.
        :param key: key of the object.
        :param data: data of the object
        :return: None
        """
        self.client.set(key, data)

    def get_object(self, key):
        """
        Get object from Redis with a key. Throws StorageNoSuchKeyError if the
        given key does not exist.
        :param key: key of the object
        :return: Data of the object
        """
        r = self.client.get(key)
        if (r is None):
            raise StorageNoSuchKeyError(key)
        return r

    def key_exists(self, key):
        """
        Check if a key exists in Redis.
        :param key: key of the object
        :return: True if key exists, False if not exists
        :rtype: boolean
        """
        return self.client.exists(key)

    def list_keys_with_prefix(self, prefix):
        """
        Return a list of keys for the given prefix.
        :param prefix: Prefix to filter object names.
        :return: List of keys in Redis that match the given prefix.
        """
        return self.client.keys(prefix + '*')
