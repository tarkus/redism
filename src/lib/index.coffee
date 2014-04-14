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
    @options.servers = ['localhost:6379'] unless @options.servers

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

    client = _servers.forEach (server) =>
      fields = server.split /:/
      client = redis.createClient parseInt(fields[1], 10), fields[0]
      client.select @options.database if @options.database
      client.auth @options.password if @options.password
      @clients[server] = client
      client

    @connected = true

    SHARDABLE.forEach (command) =>
      return if command in ['del', 'sinter']
      @[command] = @[command.toUpperCase()] = =>
        if Array.isArray arguments[0]
          key = arguments[0][0]
        else
          key = arguments[0]
        node = @nodeFor key
        client = @clients[node]
        client[command].apply client, arguments

    UNSHARDABLE.forEach (command) =>
      return if command in ['multi', 'mset', 'sinterstore', 'zinterstore']
      @[command] = @[command.toUpperCase()] = -> throw new Error "#{command} is not shardable"

  del: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray(args[0])

    self = @
    step ->
      group = @group()
      args.forEach (key, idx) ->
        node = self.nodeFor key
        client = self.clients[node]
        client.del.call client, key, group()
    , (error, groups) ->
      return callback? error if error
      assert args.length is groups.length, "wrong number of response for 'del', #{args}"
      callback? null, 'OK'

  mset: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray(args[0])

    throw new Error "wrong arguments given" unless args.length % 2 is 0
    
    self = @
    step ->
      group = @group()
      for arg, idx in args by 2
        self.set arg, args[idx+1], group()
    , (error, groups) ->
      return callback? error if error
      assert args.length / 2 is groups.length, "wrong number of response for 'mset', #{args}"
      callback? null, 'OK'

  sinter: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}
    multis = []
    migrated_keys = []

    dest_key  = args[0]
    dest_node = @nodeFor dest_key
    dest_client = @clients[dest_node]

    for arg in args[1..]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @clients[node]
      multi = multis[node] = client.multi()
      keys.forEach (key) ->
        migrated_keys.push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]

    step ->
      group = @group()
      multis.forEach (multi) -> multi.exec group()
    , (error, groups) ->
      return callback? error if error
      assert multis.length is groups.length, "wrong number of response"
      dest_client.sinter.call dest_client, args, ->
        dest_client.del migrated_keys if migrated_keys.length > 0
        callback?.apply @, arguments

  sinterstore: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}
    multis = []
    migrated_keys = []

    dest_key  = args[0]
    dest_node = @nodeFor dest_key
    dest_client = @clients[dest_node]

    for arg in args[1..]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @clients[node]
      multi = multis[node] = client.multi()
      keys.forEach (key) ->
        migrated_keys.push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]

    step ->
      group = @group()
      multis.forEach (multi) -> multi.exec group()
    , (error, groups) ->
      return callback? error if error
      assert multis.length is groups.length, "wrong number of response"
      dest_client.sinterstore.call dest_client, args, ->
        dest_client.del migrated_keys if migrated_keys.length > 0
        callback?.apply @, arguments
          
  zinterstore: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}
    multis = []
    migrated_keys = []

    dest_key  = args[0]
    dest_node = @nodeFor dest_key
    dest_client = @clients[dest_node]
    number_keys = args[1]

    for arg in args[2...2+number_keys]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @clients[node]
      multi = multis[node] = client.multi()
      keys.forEach (key) ->
        migrated_keys.push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]

    step ->
      group = @group()
      multis.forEach (multi) -> multi.exec group()
    , (error, groups) ->
      return callback? error if error
      assert multis.length is groups.length, "wrong number of response"
      dest_client.zinterstore.call dest_client, args, ->
        dest_client.del migrated_keys if migrated_keys.length > 0
        callback?.apply @, arguments

  multi: => new Multi @

  DEL: @del
  MSET: @mset
  ZADD: @zadd
  SINTER: @siner
  SINTERSTORE: @sinterstore
  ZINTERSTORE: @zinterstore
  MULTI: @multi

  nodeFor: (key) ->
    return unless key?
    assert typeof key is 'string', "wrong type of sharding key: #{key}"
    if @servers.scopes
      for scope, servers of @servers.scopes
        continue unless key.match scope
        mod = parseInt(hasher.crc32(key), 16) % servers.length
        return servers[mod]
    mod = parseInt(hasher.crc32(key), 16) % @servers.default.length
    return @servers.default[mod]

  on: (event, callback) ->
    first = @servers.default[0]
    @clients[first].on event, callback

class Multi


  constructor: (@redison) ->

    @multis = {}
    @interlachen = []
    @commands = {}
    @temp_keys = {}

    SHARDABLE.forEach (command) =>
      return if command in ['del', 'sinter']
      @[command] = @[command.toUpperCase()] = =>
        if Array.isArray arguments[0]
          key = arguments[0][0]
        else
          key = arguments[0]
        node = @redison.nodeFor key
        multi = @multis[node]
        unless multi
          multi = @multis[node] = @redison.clients[node].multi()
        @interlachen.push node
        @commands[node] ?= 0
        @commands[node] += 1
        multi[command].apply multi, arguments
        @

    UNSHARDABLE.forEach (command) =>
      return if command in ['exec', 'mset', 'sinterstore', 'zinterstore']
      @[command] = @[command.toUpperCase()] = -> throw new Error "#{command} is not supported"

  del: =>
    callback = null
    args = Array::slice.call(arguments)
    length = args.length
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray(args[0])

    args.forEach (key, idx) =>
      node = @redison.nodeFor key
      multi = @multis[node]
      unless multi
        multi = @multis[node] = @redison.clients[node].multi()
      @interlachen.push node
      @commands[node] ?= 0
      @commands[node] += 1
      multi.del.call multi, args

  mset: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray(args[0])
    throw new Error "wrong arguments given" unless args.length % 2 is 0
    
    for arg, idx in args by 2
      @set arg, args[idx+1]
    @

  sinter: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}

    dest_key  = args[0]
    dest_node = @redison.nodeFor dest_key
    dest_multi = @multis[dest_node]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redison.clients[dest_node].multi()

    for arg in args[1..]
      node = @redison.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @clients[node].multi() unless multi
      keys.forEach (key) =>
        @temp_keys[node] ?= []
        @temp_keys[node].push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]
        @interlachen.push node
        @commands[node] ?= 0
        @commands[node] += 1

    dest_multi.sinterstore.call dest_multi, args, callback
    @interlachen.push dest_node
    @commands[dest_node] ?= 0
    @commands[dest_node] += 1

  sinterstore: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}

    dest_key  = args[0]
    dest_node = @redison.nodeFor dest_key
    dest_multi = @multis[dest_node]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redison.clients[dest_node].multi()

    for arg in args[1..]
      node = @redison.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @clients[node].multi() unless multi
      keys.forEach (key) =>
        @temp_keys[node] ?= []
        @temp_keys[node].push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]
        @interlachen.push node
        @commands[node] ?= 0
        @commands[node] += 1

    dest_multi.sinterstore.call dest_multi, args
    @interlachen.push dest_node
    @commands[dest_node] ?= 0
    @commands[dest_node] += 1

  zinterstore: =>
    args = Array::slice.call(arguments)
    length = args.length
    callback = null
    callback = args.pop() if typeof args[length-1] is 'function'
    args = args[0] if Array.isArray args[0]

    remote_nodes = {}

    dest_key  = args[0]
    dest_node = @redison.nodeFor dest_key
    dest_multi = @multis[dest_node]
    number_keys = args[1]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redison.clients[dest_node].multi()

    for arg in args[2...2+number_keys]
      node = @redison.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @clients[node].multi() unless multi
      keys.forEach (key) =>
        @temp_keys[node] ?= []
        @temp_keys[node].push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db ]
        @interlachen.push node
        @commands[node] ?= 0
        @commands[node] += 1

    dest_multi.zinterstore.call dest_multi, args
    @interlachen.push dest_node
    @commands[dest_node] ?= 0
    @commands[dest_node] += 1

  exec: (callback) =>
    nodes = Object.keys @multis
    self = @
    step ->
      group = @group()
      nodes.forEach (node) -> self.multis[node].exec group()
    , (error, groups) ->
      self.redison[node].del keys for node, keys of self.temp_keys
      return callback? error if error
      assert nodes.length is groups.length, "wrong number of response"
      results = []
      groups.forEach (results, index) ->
        node = nodes[index]
        assert results.length is self.commands[node], "#{node} is missing results"
      self.interlachen.forEach (node) ->
        index = nodes.indexOf node
        results.push groups[index].shift()
      callback? null, results

  DEL: @del
  MSET: @mset
  SINTER: @siner
  SINTERSTORE: @sinterstore
  ZINTERSTORE: @zinterstore
  EXEC: @exec

module.exports = Redison
