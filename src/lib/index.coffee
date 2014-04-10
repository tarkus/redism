_      = require 'underscore'
assert = require 'assert'
redis  = require 'redis'
step   = require 'step'
hasher = require './hasher'

SHARDABLE = [
  "append", "bitcount", "blpop", "brpop", "debug object", "decr", "decrby", "del", "dump", "exists", "expire",
  "expireat", "get", "getbit", "getrange", "getset", "hdel", "hexists", "hget", "hgetall", "hincrby",
  "hincrbyfloat", "hkeys", "hlen", "hmget", "hmset", "hset", "hsetnx", "hvals", "incr", "incrby", "incrbyfloat",
  "lindex", "linsert", "llen", "lpop", "lpush", "lpushx", "lrange", "lrem", "lset", "ltrim", "mget", "move",
  "persist", "pexpire", "pexpireat", "psetex", "pttl", "rename", "renamenx", "restore", "rpop", "rpush", "rpushx",
  "sadd", "scard", "sdiff", "set", "setbit", "setex", "setnx", "setrange", "sinter", "sismember", "smembers",
  "sort", "spop", "srandmember", "srem", "strlen", "sunion", "ttl", "type", "watch", "zadd", "zcard", "zcount",
  "zincrby", "zrange", "zrangebyscore", "zrank", "zrem", "zremrangebyrank", "zremrangebyscore", "zrevrange",
  "zrevrangebyscore", "zrevrank", "zscore"
]

UNSHARDABLE = [
  "auth", "bgrewriteaof", "bgsave", "bitop", "brpoplpush", "client kill", "client list", "client getname",
  "client setname", "config get", "config set", "config resetstat", "dbsize", "debug segfault", "discard",
  "echo", "eval", "evalsha", "exec", "flushall", "flushdb", "info", "keys", "lastsave", "migrate", "monitor",
  "mset", "msetnx", "multi", "object", "ping", "psubscribe", "publish", "punsubscribe", "quit", "randomkey",
  "rpoplpush", "save", "script exists", "script flush", "script kill", "script load", "sdiffstore", "select",
  "shutdown", "sinterstore", "slaveof", "slowlog", "smove", "subscribe", "sunionstore", "sync", "time",
  "unsubscribe", "unwatch", "zinterstore", "zunionstore"
]

###
# Simple options
# {
#   servers: [ 'localhost:6379', 'localhost:6479' ]
#   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
#   database: 3
# }
# 
# Options with scopes
#
# {
#   servers: [
#     [ ':hash:', [ 'localhost:6579', 'localhost:6679' ] ]
#     [ 'localhost:6379', 'localhost:6479' ],
#   ]
#   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
#   database: 3
# }
#
###
class Redison

  constructor: (@options) ->
    assert !!@options, "options must be an object"
    @options.servers = ['localhost:6379'] unless @options.servers)

    @clients = {}
    @servers =
      default: null
      scopes: {}
    _servers = []

    unless typeof @options.servers[0] is 'string' # Options with scopes
      for scoped in @options.servers
        if Array.isArray scoped[1] # Specifed scope
          @servers.scopes[scoped[0]] = scoped[1]
          _servers = _.union _servers, scoped[1]
        else # Default scope
          @servers.default = scoped
          _servers = _.union _servers, scoped
    else # Simple options
      @servers.default = @options.servers
      _servers = @options.servers

    assert Array.isArray(@servers.default), "default servers must be set"

    _servers.forEach (server) =>
      fields = server.split /:/
      client = redis.createClient parseInt(fields[1], 10), fields[0]
      client.select @options.database if @options.database
      client.auth @options.password if @options.password
      @clients[server] = client

    SHARDABLE.forEach (command) =>
      @[command] = =>
        node = @nodeFor arguments[0]
        client = @clients[node]
        client[command].apply client, arguments

    UNSHARDABLE.forEach (command) =>
      return if command is 'multi'
      @[command] = -> throw new Error "#{command} is not shardable"

  multi: => new Multi @

  nodeFor: (key) ->
    if @servers.scopes
      for scope, servers of @servers.scopes
        continue unless key.match scope
        mod = parseInt(hasher.crc32(key), 16) % servers.length
        return servers[mod]
    mod = parseInt(hasher.crc32(key), 16) % @servers.default.length
    return @servers.default[mod]

class Multi

  multis: {}
  interlachen: []
  counter: {}

  constructor: (@redison) ->
    SHARDABLE.forEach (command) =>
      @[command] = =>
        node = @redison.nodeFor arguments[0]
        multi = @multis[node]
        unless multi
          multi = @multis[node] = @redison.clients[node].multi()
        @interlachen.push node
        @counter[node] = 0 unless @counter[node]?
        @counter[node] += 1
        multi[command].apply multi, arguments
        @

    UNSHARDABLE.forEach (command) =>
      return if command is 'exec'
      @[command] = -> throw new Error "#{command} is not supported"

  exec: (callback) =>
    nodes = Object.keys @multis
    self = @
    step ->
      group = @group()
      nodes.forEach (node) -> self.multis[node].exec group()
    , (error, groups) ->
      return callback error if error
      assert nodes.length is groups.length, "wrong number of response"
      results = []
      groups.forEach (results, index) ->
        node = nodes[index]
        assert results.length is self.counter[node], "#{node} is missing results"
      self.interlachen.forEach (node) ->
        index = nodes.indexOf node
        results.push groups[index].shift()
      callback? null, results

module.exports = Redison
