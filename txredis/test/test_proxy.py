
from txredis.test import test_redis
from txredis.proxy import RedisReconnectingProxy

class RedisReconnectingProxyMixin:
    def setUp(self):
        self.redis = RedisReconnectingProxy(test_redis.REDIS_HOST, test_redis.REDIS_PORT)

    def tearDown(self):
        self.redis._cleanup()

class General(RedisReconnectingProxyMixin, test_redis.General):
    pass

class Strings(RedisReconnectingProxyMixin, test_redis.Strings):
    pass

class Lists(RedisReconnectingProxyMixin, test_redis.Lists):
    pass

class Sets(RedisReconnectingProxyMixin, test_redis.Sets):
    pass

class Hash(RedisReconnectingProxyMixin, test_redis.Hash):
    pass

class LargeMultiBulk(RedisReconnectingProxyMixin, test_redis.LargeMultiBulk):
    pass

class SortedSet(RedisReconnectingProxyMixin, test_redis.SortedSet):
    pass
