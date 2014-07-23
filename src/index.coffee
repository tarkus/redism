_      = require 'underscore'
assert = require 'assert'
redis  = require 'redis'
step   = require 'step'
hasher = require './hasher'
url    = require 'url'

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
#   nodes: [ 'redis://localhost:6379/3', 'redis://localhost:6479/3' ]
#   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
# }
# 
# Options with scopes
#
# {
#   nodes: [
#     [ ':hash:', [ 'redis://localhost:6579', 'redis://localhost:6679/3' ] ]
#     [ 'redis://localhost:6379', 'redis://localhost:6479' ],
#   ]
#   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
# }
#
###
class Redism

  shardable: true   # Just a flag

  constructor: (@options) ->
    @ready = false

    @options ?= {}
    @options.name    ?= "Redism instance"   # Set a name for connection report
    @options.nodes   ?= @options.servers    # Alias nodes as servers in options
    @options.nodes   ?= ['redis://localhost:6379/0']
    @options.instant ?= false   # We don't connect to node instantly by default

    @clients = {}       # Redis client hash => "redis://localhost:6379/0": [ Redis client object ] 
    @node_list = []     # Node name array
    @server_list = {}   # Server name hash => "localhost:6379": 2
    @connectors = {}    # Connector for connecting to node

    # Structured node hash
    @nodes = { scopes: {}, default: null }

    # Parse 'nodes' parameter
    if typeof @options.nodes[0] is 'string'
      # Simple options => [ 'redis://1.2.3.4:5678/9' ]
      @nodes.default = @options.nodes
      @node_list = @options.nodes
    else # Options with scopes
      for scope in @options.nodes

        # Expect
        #     [ "xxx", ['redis://1.2.3.4:5678/9'] ]
        #   Or
        #     [ 'redis://1.2.3.4:5678/9' ]
        
        if Array.isArray scope[1] # Specifed scope
          [ name, nodes ] = scope
          @nodes.scopes[name] = nodes
        else # Default scope
          nodes = scope
          @nodes.default = nodes

        @node_list = _.union @node_list, nodes

    connected = 0
    total = @node_list.length

    connection_check = =>
      connected += 1
      if connected is total
        @ready = true
        console.log "#{@options.name}: #{connected} nodes connected"

    console.log "#{@options.name}: #{total} nodes registered"

    @node_list.forEach (node) =>
      nodeparts = url.parse node
      return console.error "Please use redis url instead #{node}" unless nodeparts.protocol is 'redis:'

      host = nodeparts.hostname
      port = parseInt(nodeparts.port) or '6379'
      db = nodeparts.pathname?.slice 1 or null
      pass = null

      if nodeparts.auth
        authparts = nodeparts.auth.split ":"
        pass = authparts[1] if authparts

      @connectors[node] = =>
        client = redis.createClient port, host, no_ready_check: true
        client.select db if db
        client.auth pass if pass
        @clients[node] = client
        @server_list["#{host}:#{port}"] or= 0
        @server_list["#{host}:#{port}"] += 1
        return client

      if @options.instant
        client = @connectors[node]()
        client.on 'connect', connection_check

    # Makr it as ready for on-demands
    @ready = true unless @options.instant

    SHARDABLE.forEach (command) =>
      return if command in ['del', 'sinter']
      @[command] = @[command.toUpperCase()] = =>
        if Array.isArray arguments[0]
          key = arguments[0][0]
        else
          key = arguments[0]
        node = @nodeFor key
        client = @getClient node
        client[command].apply client, arguments

    UNSHARDABLE.forEach (command) =>
      return if command in ['multi', 'mset', 'sinterstore', 'zinterstore']
      @[command] = @[command.toUpperCase()] = -> throw new Error "#{command} is not shardable"

  isReady: -> @ready

  on: (event, callback) ->
    first = @nodes.default[0]
    @getClient(first).on event, callback

  nodeFor: (key) ->
    return unless key?
    assert typeof key is 'string', "wrong type of sharding key: #{key}"
    if @nodes.scopes
      for scope, nodes of @nodes.scopes
        continue unless key.match scope
        mod = parseInt(hasher.crc32(key), 16) % nodes.length
        return nodes[mod]
    mod = parseInt(hasher.crc32(key), 16) % @nodes.default.length
    return @nodes.default[mod]

  getNode: (node, callback) ->
    client = @clients[node]
    client = @connectors[node]() unless client
    client.on 'connect', callback.bind(client) if callback?
    client

  getClient: => @getNode.apply @, arguments

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
        client = self.getClient(node)
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
    dest_client = @getClient dest_node

    for arg in args[1..]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @getClient node
      multi = multis[node] = client.multi()
      keys.forEach (key) ->
        migrated_keys.push key
        multi.migrate [
          dest_node.host, dest_node.port,
          key, dest_node.selected_db,  ]

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
    dest_client = @getClient dest_node

    for arg in args[1..]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @getClient node
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
    dest_client = @getClient dest_node
    number_keys = args[1]

    for arg in args[2...2+number_keys]
      node = @nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      client = @getClient node
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


class Multi

  constructor: (@redism) ->

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
        node = @redism.nodeFor key
        multi = @multis[node]
        unless multi
          multi = @multis[node] = @redism.getClient(node).multi()
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
      node = @redism.nodeFor key
      multi = @multis[node]
      unless multi
        multi = @multis[node] = @redism.getClient(node).multi()
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
    dest_node = @redism.nodeFor dest_key
    dest_multi = @multis[dest_node]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redism.getClient(dest_node).multi()

    for arg in args[1..]
      node = @redism.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @getClient(node).multi() unless multi
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
    dest_node = @redism.nodeFor dest_key
    dest_multi = @multis[dest_node]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redism.getClient(dest_node).multi()

    for arg in args[1..]
      node = @redism.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @redism.getClient(node).multi() unless multi
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
    dest_node = @redism.nodeFor dest_key
    dest_multi = @multis[dest_node]
    number_keys = args[1]
    unless dest_multi
      dest_multi = @multis[dest_node] = @redism.getClient(dest_node).multi()

    for arg in args[2...2+number_keys]
      node = @redism.nodeFor arg
      remote_nodes[node] ?= []
      remote_nodes[node].push arg

    for node, keys of remote_nodes
      continue if node is dest_node
      multi = @multis[node]
      multi = @multis[node] = @redism.getClient(node).multi() unless multi
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
      self.redism[node].del keys for node, keys of self.temp_keys
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

module.exports = Redism
