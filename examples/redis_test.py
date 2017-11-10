import pywren

from pywren import RedisBackend

redis_config = {'endpoint': 'redis-test.qituln.0001.usw1.cache.amazonaws.com',
                'port': '6379'}

def my_function(x):
    redis_storage = RedisBackend(redis_config)
    redis_storage.put_object("foo", x+7)
    value = redis_storage.get_object("foo")
    return value

wrenexec = pywren.default_executor()
future = wrenexec.call_async(my_function, 3)

print future.result()
