
from txredis.test import test_redis
from txredis.proxy import RedisReconnectingProxy
from twisted.trial import unittest
from twisted.internet import defer

class RedisReconnectingProxyMixin:
    def setUp(self):
        self.redis = RedisReconnectingProxy(test_redis.REDIS_HOST, test_redis.REDIS_PORT)

    def tearDown(self):
        self.redis._cleanup()

class TestProxy(RedisReconnectingProxyMixin, unittest.TestCase):

    @defer.inlineCallbacks
    def test_reconnect(self):
        a = yield self.redis.ping()
        self.assertEqual(a, 'PONG')

        self.redis.connection.transport.loseConnection()

        a = yield self.redis.ping()
        self.assertEqual(a, 'PONG')
        self.flushLoggedErrors('twisted.internet.error.ConnectionDone')

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
