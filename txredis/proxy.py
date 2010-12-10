"""
Safe reconnecting proxy over RedisClient.
"""

from twisted.internet import defer, protocol, reactor
from twisted.python import failure, log

from txredis.protocol import Redis


class WaitingDeferred(object):
    """
    Anti-DeferredList.

    We have some resource, and several actors waiting
    on it. Every actor should get notified via Deferred
    about resource availability.

    @ivar queue: queue of waiting Deferreds (actors)
    @type queue: C{list}
    @ivar fired: was this Deferred fired?
    @type fired: C{bool}
    """

    def __init__(self):
        """
        Constructor.
        """
        self.queue = []
        self.fired = False

    def push(self):
        """
        One more actor wants to get the resource.

        Give him Deferred!

        @return: Deferred, resulting in resource
        @rtype: C{Deferred}
        """
        assert not self.fired

        d = defer.Deferred()
        self.queue.append(d)
        return d

    def callback(self, *args, **kwargs):
        """
        We got resource, pass it to all waiting actors.
        """
        assert not self.fired

        self.fired = True

        for d in self.queue:
            d.callback(*args, **kwargs)

    def errback(self, *args, **kwargs):
        """
        We got error, propagate it to actors.
        """
        assert not self.fired

        self.fired = True

        for d in self.queue:
            d.errback(*args, **kwargs)


def _method_template(method):
    """
    Error handling wrapper around Redis operations.

    @param method: method name
    @type method: C{str}
    """

    def _method(self, *args, **kwargs):
        attempt = kwargs.pop('attempt', 1)

        def trapFailure(fail):
            if fail.check("txredis.protocol.ResponseError") or attempt >= self.max_attempts:
                return fail

            log.err(fail, "Failure in %r, reconnect is attempted (attempt=%d)" % (self, attempt))

            if self.connection is not None:
                self.connection.transport.loseConnection()
            self.connection = None

            d = defer.Deferred()
            reactor.callLater(self.timeout_retry, d.callback, None)
            d.addCallback(lambda _: getattr(self, method)(*args, attempt=attempt+1, **kwargs))

            return d

        return self._get_connection().addCallback(lambda connection: getattr(connection, method)(*args, **kwargs)) \
                .addErrback(trapFailure)

    _method.__name__ = method
    return _method


class RedisReconnectingProxy(object):
    """
    I feel like always connected Redis protocol instance. 
    
    If disconnect happens, and in case of any other error, I perform automatic
    and transparent reconnect.

    @ivar host: Redis host
    @type host: C{str}
    @ivar port: Redis port
    @type port: C{int}
    @ivar connection: connection (protocol) to Redis
    @type connection: L{Redis}
    @ivar timeout_retry: timeout for reconnect and operation retry (sec)
    @type timeout_retry: C{int}
    @ivar timeout_operation: timeout for Redis operation (sec)
    @type timeout_oeration: C{int}
    @ivar max_attempts: maximum number of attempts 
    @type max_attempts: C{int}
    """

    def __init__(self, host, port, timeout_retry=5, timeout_operation=180, max_attempts=3):
        """
        Constructor.

        @param host: Redis host
        @type host: C{str}
        @param port: Redis port
        @type port: C{int}
        @param timeout_retry: timeout for reconnect and operation retry (sec)
        @type timeout_retry: C{int}
        @param timeout_operation: timeout for Redis operation (sec)
        @type timeout_oeration: C{int}
        @param max_attempts: maximum number of attempts 
        @type max_attempts: C{int}
        """
        self.host = host
        self.port = port
        self.timeout_retry = timeout_retry
        self.timeout_operation = timeout_operation
        self.max_attempts = max_attempts
        self.connection = None

    def _cleanup(self):
        """
        Cleanup everything (for tests).
        """
        if self.connection is not None:
            self.connection.transport.loseConnection()
            self.connection = None

    def __repr__(self):
        return "<RedisReconnectingProxy(%r:%r)>" % (self.host, self.port)

    def _get_connection(self):
        """
        Build and return connection to Result.

        @return: Deferred, resulting in connection
        @rtype: C{Deferred}
        """
        if self.connection is not None:
            return defer.succeed(self.connection)

        if getattr(self, 'waiter', None) is not None:
            return self.waiter.push()

        self.waiter = WaitingDeferred()

        def gotProtocol(protocol):
            self.connection = protocol
            waiter = self.waiter
            del self.waiter

            waiter.callback(protocol)

        def errback(f):
            waiter = self.waiter
            del self.waiter

            waiter.errback(f)

        protocol.ClientCreator(reactor, Redis).connectTCP(self.host, self.port, timeout=self.timeout_operation).addCallbacks(gotProtocol, errback)

        return self.waiter.push()


for method in ['ping', 'get_config', 'set_config', 'get', 'set', 'mset', 'incr', 'append', 'substr', 'getset', 'mget', 'decr', 'exists', 'delete', 'get_type',
               'keys', 'randomkey', 'rename', 'dbsize', 'expire', 'ttl', 'push', 'llen', 'lrange', 'ltrim', 'lindex', 'pop', 'bpop', 'rpoplpush', 'lset', 'lrem',
               'sadd', 'srem', 'spop', 'scard', 'sismember', 'sdiff', 'sdiffstore', 'srandmember', 'sinter', 'sinterstore', 'smembers', 'smove', 'sunion',
               'sunionstore', 'hmset', 'hset', 'hget', 'hmget', 'hkeys', 'hvals', 'hincr', 'hexists', 'hdelete', 'hlen', 'hgetall', 'publish',
               'zadd', 'zrem', 'zremrangebyscore', 'zremrangebyrank', 'zunionstore', 'zinterstore', 'zincr', 'zrank', 'zcount', 'zrange', 'zrevrange',
               'zrangebyscore', 'zcard', 'zscore', 'flush', 'execute', 'select', 'save', 'multi', 'info', 'move', 'lastsave', 'discard', 'sort']:
    setattr(RedisReconnectingProxy, method, _method_template(method))
