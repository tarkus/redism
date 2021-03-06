// Generated by CoffeeScript 1.7.1
var Multi, Redism, SHARDABLE, UNSHARDABLE, assert, hasher, redis, step, url, _,
  __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };

_ = require('underscore');

assert = require('assert');

redis = require('redis');

step = require('step');

hasher = require('./hasher');

url = require('url');

SHARDABLE = ["append", "bitcount", "blpop", "brpop", "debug object", "decr", "decrby", "del", "dump", "exists", "expire", "expireat", "get", "getbit", "getrange", "getset", "hdel", "hexists", "hget", "hgetall", "hincrby", "hincrbyfloat", "hkeys", "hlen", "hmget", "hmset", "hset", "hsetnx", "hvals", "incr", "incrby", "incrbyfloat", "lindex", "linsert", "llen", "lpop", "lpush", "lpushx", "lrange", "lrem", "lset", "ltrim", "mget", "move", "persist", "pexpire", "pexpireat", "psetex", "pttl", "rename", "renamenx", "restore", "rpop", "rpush", "rpushx", "sadd", "scard", "sdiff", "set", "setbit", "setex", "setnx", "setrange", "sinter", "sismember", "smembers", "sort", "spop", "srandmember", "srem", "strlen", "sunion", "ttl", "type", "watch", "zadd", "zcard", "zcount", "zincrby", "zrange", "zrangebyscore", "zrank", "zrem", "zremrangebyrank", "zremrangebyscore", "zrevrange", "zrevrangebyscore", "zrevrank", "zscore"];

UNSHARDABLE = ["auth", "bgrewriteaof", "bgsave", "bitop", "brpoplpush", "client kill", "client list", "client getname", "client setname", "config get", "config set", "config resetstat", "dbsize", "debug segfault", "discard", "echo", "eval", "evalsha", "exec", "flushall", "flushdb", "info", "keys", "lastsave", "migrate", "monitor", "mset", "msetnx", "multi", "object", "ping", "psubscribe", "publish", "punsubscribe", "quit", "randomkey", "rpoplpush", "save", "script exists", "script flush", "script kill", "script load", "sdiffstore", "select", "shutdown", "sinterstore", "slaveof", "slowlog", "smove", "subscribe", "sunionstore", "sync", "time", "unsubscribe", "unwatch", "zinterstore", "zunionstore"];


/*
 * Simple options
 * {
 *   nodes: [ 'redis://localhost:6379/3', 'redis://localhost:6479/3' ]
 *   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
 * }
 * 
 * Options with scopes
 *
 * {
 *   nodes: [
 *     [ ':hash:', [ 'redis://localhost:6579', 'redis://localhost:6679/3' ] ]
 *     [ 'redis://localhost:6379', 'redis://localhost:6479' ],
 *   ]
 *   password: 'SxZRihb3A5LB6XtrmIU7XOgBAndBbhW47pxx'
 * }
 *
 */

Redism = (function() {
  Redism.prototype.shardable = true;

  function Redism(options) {
    var connected, connection_check, name, nodes, scope, total, _base, _base1, _base2, _base3, _i, _len, _ref;
    this.options = options;
    this.multi = __bind(this.multi, this);
    this.zinterstore = __bind(this.zinterstore, this);
    this.sinterstore = __bind(this.sinterstore, this);
    this.sinter = __bind(this.sinter, this);
    this.mset = __bind(this.mset, this);
    this.del = __bind(this.del, this);
    this.getClient = __bind(this.getClient, this);
    this.ready = false;
    if (this.options == null) {
      this.options = {};
    }
    if ((_base = this.options).name == null) {
      _base.name = "Redism instance";
    }
    if ((_base1 = this.options).nodes == null) {
      _base1.nodes = this.options.servers;
    }
    if ((_base2 = this.options).nodes == null) {
      _base2.nodes = ['redis://localhost:6379/0'];
    }
    if ((_base3 = this.options).instant == null) {
      _base3.instant = false;
    }
    this.clients = {};
    this.node_list = [];
    this.server_list = {};
    this.connectors = {};
    this.nodes = {
      scopes: {},
      "default": null
    };
    if (typeof this.options.nodes[0] === 'string') {
      this.nodes["default"] = this.options.nodes;
      this.node_list = this.options.nodes;
    } else {
      _ref = this.options.nodes;
      for (_i = 0, _len = _ref.length; _i < _len; _i++) {
        scope = _ref[_i];
        if (Array.isArray(scope[1])) {
          name = scope[0], nodes = scope[1];
          this.nodes.scopes[name] = nodes;
        } else {
          nodes = scope;
          this.nodes["default"] = nodes;
        }
        this.node_list = _.union(this.node_list, nodes);
      }
    }
    connected = 0;
    total = this.node_list.length;
    connection_check = (function(_this) {
      return function() {
        connected += 1;
        if (connected === total) {
          _this.ready = true;
          return console.log("" + _this.options.name + ": " + connected + " nodes connected");
        }
      };
    })(this);
    console.log("" + this.options.name + ": " + total + " nodes registered");
    this.node_list.forEach((function(_this) {
      return function(node) {
        var authparts, client, db, host, nodeparts, pass, port, _ref1;
        nodeparts = url.parse(node);
        if (nodeparts.protocol !== 'redis:') {
          return console.error("Please use redis url instead " + node);
        }
        host = nodeparts.hostname;
        port = parseInt(nodeparts.port) || '6379';
        db = (_ref1 = nodeparts.pathname) != null ? _ref1.slice(1 || null) : void 0;
        pass = null;
        if (nodeparts.auth) {
          authparts = nodeparts.auth.split(":");
          if (authparts) {
            pass = authparts[1];
          }
        }
        _this.connectors[node] = function() {
          var client, _base4, _name;
          client = redis.createClient(port, host, {
            no_ready_check: true
          });
          if (db) {
            client.select(db);
          }
          if (pass) {
            client.auth(pass);
          }
          _this.clients[node] = client;
          (_base4 = _this.server_list)[_name = "" + host + ":" + port] || (_base4[_name] = 0);
          _this.server_list["" + host + ":" + port] += 1;
          return client;
        };
        if (_this.options.instant) {
          client = _this.connectors[node]();
          return client.on('connect', connection_check);
        }
      };
    })(this));
    if (!this.options.instant) {
      this.ready = true;
    }
    SHARDABLE.forEach((function(_this) {
      return function(command) {
        if (command === 'del' || command === 'sinter') {
          return;
        }
        return _this[command] = _this[command.toUpperCase()] = function() {
          var client, key, node;
          if (Array.isArray(arguments[0])) {
            key = arguments[0][0];
          } else {
            key = arguments[0];
          }
          node = _this.nodeFor(key);
          client = _this.getClient(node);
          return client[command].apply(client, arguments);
        };
      };
    })(this));
    UNSHARDABLE.forEach((function(_this) {
      return function(command) {
        if (command === 'multi' || command === 'mset' || command === 'sinterstore' || command === 'zinterstore') {
          return;
        }
        return _this[command] = _this[command.toUpperCase()] = function() {
          throw new Error("" + command + " is not shardable");
        };
      };
    })(this));
  }

  Redism.prototype.isReady = function() {
    return this.ready;
  };

  Redism.prototype.on = function(event, callback) {
    var first;
    first = this.nodes["default"][0];
    return this.getClient(first).on(event, callback);
  };

  Redism.prototype.nodeFor = function(key) {
    var mod, nodes, scope, _ref;
    if (key == null) {
      return;
    }
    assert(typeof key === 'string', "wrong type of sharding key: " + key);
    if (this.nodes.scopes) {
      _ref = this.nodes.scopes;
      for (scope in _ref) {
        nodes = _ref[scope];
        if (!key.match(scope)) {
          continue;
        }
        mod = parseInt(hasher.crc32(key), 16) % nodes.length;
        return nodes[mod];
      }
    }
    mod = parseInt(hasher.crc32(key), 16) % this.nodes["default"].length;
    return this.nodes["default"][mod];
  };

  Redism.prototype.getNode = function(node, callback) {
    var client;
    client = this.clients[node];
    if (!client) {
      client = this.connectors[node]();
    }
    if (callback != null) {
      client.on('connect', callback.bind(client));
    }
    return client;
  };

  Redism.prototype.getClient = function() {
    return this.getNode.apply(this, arguments);
  };

  Redism.prototype.del = function() {
    var args, callback, length, self;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    self = this;
    return step(function() {
      var group;
      group = this.group();
      return args.forEach(function(key, idx) {
        var client, node;
        node = self.nodeFor(key);
        client = self.getClient(node);
        return client.del.call(client, key, group());
      });
    }, function(error, groups) {
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(args.length === groups.length, "wrong number of response for 'del', " + args);
      return typeof callback === "function" ? callback(null, 'OK') : void 0;
    });
  };

  Redism.prototype.mset = function() {
    var args, callback, length, self;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    if (args.length % 2 !== 0) {
      throw new Error("wrong arguments given");
    }
    self = this;
    return step(function() {
      var arg, group, idx, _i, _len, _results;
      group = this.group();
      _results = [];
      for (idx = _i = 0, _len = args.length; _i < _len; idx = _i += 2) {
        arg = args[idx];
        _results.push(self.set(arg, args[idx + 1], group()));
      }
      return _results;
    }, function(error, groups) {
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(args.length / 2 === groups.length, "wrong number of response for 'mset', " + args);
      return typeof callback === "function" ? callback(null, 'OK') : void 0;
    });
  };

  Redism.prototype.sinter = function() {
    var arg, args, callback, client, dest_client, dest_key, dest_node, keys, length, migrated_keys, multi, multis, node, remote_nodes, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    multis = [];
    migrated_keys = [];
    dest_key = args[0];
    dest_node = this.nodeFor(dest_key);
    dest_client = this.getClient(dest_node);
    _ref = args.slice(1);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      client = this.getClient(node);
      multi = multis[node] = client.multi();
      keys.forEach(function(key) {
        migrated_keys.push(key);
        return multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
      });
    }
    return step(function() {
      var group;
      group = this.group();
      return multis.forEach(function(multi) {
        return multi.exec(group());
      });
    }, function(error, groups) {
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(multis.length === groups.length, "wrong number of response");
      return dest_client.sinter.call(dest_client, args, function() {
        if (migrated_keys.length > 0) {
          dest_client.del(migrated_keys);
        }
        return callback != null ? callback.apply(this, arguments) : void 0;
      });
    });
  };

  Redism.prototype.sinterstore = function() {
    var arg, args, callback, client, dest_client, dest_key, dest_node, keys, length, migrated_keys, multi, multis, node, remote_nodes, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    multis = [];
    migrated_keys = [];
    dest_key = args[0];
    dest_node = this.nodeFor(dest_key);
    dest_client = this.getClient(dest_node);
    _ref = args.slice(1);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      client = this.getClient(node);
      multi = multis[node] = client.multi();
      keys.forEach(function(key) {
        migrated_keys.push(key);
        return multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
      });
    }
    return step(function() {
      var group;
      group = this.group();
      return multis.forEach(function(multi) {
        return multi.exec(group());
      });
    }, function(error, groups) {
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(multis.length === groups.length, "wrong number of response");
      return dest_client.sinterstore.call(dest_client, args, function() {
        if (migrated_keys.length > 0) {
          dest_client.del(migrated_keys);
        }
        return callback != null ? callback.apply(this, arguments) : void 0;
      });
    });
  };

  Redism.prototype.zinterstore = function() {
    var arg, args, callback, client, dest_client, dest_key, dest_node, keys, length, migrated_keys, multi, multis, node, number_keys, remote_nodes, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    multis = [];
    migrated_keys = [];
    dest_key = args[0];
    dest_node = this.nodeFor(dest_key);
    dest_client = this.getClient(dest_node);
    number_keys = args[1];
    _ref = args.slice(2, 2 + number_keys);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      client = this.getClient(node);
      multi = multis[node] = client.multi();
      keys.forEach(function(key) {
        migrated_keys.push(key);
        return multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
      });
    }
    return step(function() {
      var group;
      group = this.group();
      return multis.forEach(function(multi) {
        return multi.exec(group());
      });
    }, function(error, groups) {
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(multis.length === groups.length, "wrong number of response");
      return dest_client.zinterstore.call(dest_client, args, function() {
        if (migrated_keys.length > 0) {
          dest_client.del(migrated_keys);
        }
        return callback != null ? callback.apply(this, arguments) : void 0;
      });
    });
  };

  Redism.prototype.multi = function() {
    return new Multi(this);
  };

  Redism.prototype.DEL = Redism.del;

  Redism.prototype.MSET = Redism.mset;

  Redism.prototype.ZADD = Redism.zadd;

  Redism.prototype.SINTER = Redism.siner;

  Redism.prototype.SINTERSTORE = Redism.sinterstore;

  Redism.prototype.ZINTERSTORE = Redism.zinterstore;

  Redism.prototype.MULTI = Redism.multi;

  return Redism;

})();

Multi = (function() {
  function Multi(redism) {
    this.redism = redism;
    this.exec = __bind(this.exec, this);
    this.zinterstore = __bind(this.zinterstore, this);
    this.sinterstore = __bind(this.sinterstore, this);
    this.sinter = __bind(this.sinter, this);
    this.mset = __bind(this.mset, this);
    this.del = __bind(this.del, this);
    this.multis = {};
    this.interlachen = [];
    this.commands = {};
    this.temp_keys = {};
    SHARDABLE.forEach((function(_this) {
      return function(command) {
        if (command === 'del' || command === 'sinter') {
          return;
        }
        return _this[command] = _this[command.toUpperCase()] = function() {
          var key, multi, node, _base;
          if (Array.isArray(arguments[0])) {
            key = arguments[0][0];
          } else {
            key = arguments[0];
          }
          node = _this.redism.nodeFor(key);
          multi = _this.multis[node];
          if (!multi) {
            multi = _this.multis[node] = _this.redism.getClient(node).multi();
          }
          _this.interlachen.push(node);
          if ((_base = _this.commands)[node] == null) {
            _base[node] = 0;
          }
          _this.commands[node] += 1;
          multi[command].apply(multi, arguments);
          return _this;
        };
      };
    })(this));
    UNSHARDABLE.forEach((function(_this) {
      return function(command) {
        if (command === 'exec' || command === 'mset' || command === 'sinterstore' || command === 'zinterstore') {
          return;
        }
        return _this[command] = _this[command.toUpperCase()] = function() {
          throw new Error("" + command + " is not supported");
        };
      };
    })(this));
  }

  Multi.prototype.del = function() {
    var args, callback, length;
    callback = null;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    return args.forEach((function(_this) {
      return function(key, idx) {
        var multi, node, _base;
        node = _this.redism.nodeFor(key);
        multi = _this.multis[node];
        if (!multi) {
          multi = _this.multis[node] = _this.redism.getClient(node).multi();
        }
        _this.interlachen.push(node);
        if ((_base = _this.commands)[node] == null) {
          _base[node] = 0;
        }
        _this.commands[node] += 1;
        return multi.del.call(multi, args);
      };
    })(this));
  };

  Multi.prototype.mset = function() {
    var arg, args, callback, idx, length, _i, _len;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    if (args.length % 2 !== 0) {
      throw new Error("wrong arguments given");
    }
    for (idx = _i = 0, _len = args.length; _i < _len; idx = _i += 2) {
      arg = args[idx];
      this.set(arg, args[idx + 1]);
    }
    return this;
  };

  Multi.prototype.sinter = function() {
    var arg, args, callback, dest_key, dest_multi, dest_node, keys, length, multi, node, remote_nodes, _base, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    dest_key = args[0];
    dest_node = this.redism.nodeFor(dest_key);
    dest_multi = this.multis[dest_node];
    if (!dest_multi) {
      dest_multi = this.multis[dest_node] = this.redism.getClient(dest_node).multi();
    }
    _ref = args.slice(1);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.redism.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      multi = this.multis[node];
      if (!multi) {
        multi = this.multis[node] = this.getClient(node).multi();
      }
      keys.forEach((function(_this) {
        return function(key) {
          var _base, _base1;
          if ((_base = _this.temp_keys)[node] == null) {
            _base[node] = [];
          }
          _this.temp_keys[node].push(key);
          multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
          _this.interlachen.push(node);
          if ((_base1 = _this.commands)[node] == null) {
            _base1[node] = 0;
          }
          return _this.commands[node] += 1;
        };
      })(this));
    }
    dest_multi.sinterstore.call(dest_multi, args, callback);
    this.interlachen.push(dest_node);
    if ((_base = this.commands)[dest_node] == null) {
      _base[dest_node] = 0;
    }
    return this.commands[dest_node] += 1;
  };

  Multi.prototype.sinterstore = function() {
    var arg, args, callback, dest_key, dest_multi, dest_node, keys, length, multi, node, remote_nodes, _base, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    dest_key = args[0];
    dest_node = this.redism.nodeFor(dest_key);
    dest_multi = this.multis[dest_node];
    if (!dest_multi) {
      dest_multi = this.multis[dest_node] = this.redism.getClient(dest_node).multi();
    }
    _ref = args.slice(1);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.redism.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      multi = this.multis[node];
      if (!multi) {
        multi = this.multis[node] = this.redism.getClient(node).multi();
      }
      keys.forEach((function(_this) {
        return function(key) {
          var _base, _base1;
          if ((_base = _this.temp_keys)[node] == null) {
            _base[node] = [];
          }
          _this.temp_keys[node].push(key);
          multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
          _this.interlachen.push(node);
          if ((_base1 = _this.commands)[node] == null) {
            _base1[node] = 0;
          }
          return _this.commands[node] += 1;
        };
      })(this));
    }
    dest_multi.sinterstore.call(dest_multi, args);
    this.interlachen.push(dest_node);
    if ((_base = this.commands)[dest_node] == null) {
      _base[dest_node] = 0;
    }
    return this.commands[dest_node] += 1;
  };

  Multi.prototype.zinterstore = function() {
    var arg, args, callback, dest_key, dest_multi, dest_node, keys, length, multi, node, number_keys, remote_nodes, _base, _i, _len, _ref;
    args = Array.prototype.slice.call(arguments);
    length = args.length;
    callback = null;
    if (typeof args[length - 1] === 'function') {
      callback = args.pop();
    }
    if (Array.isArray(args[0])) {
      args = args[0];
    }
    remote_nodes = {};
    dest_key = args[0];
    dest_node = this.redism.nodeFor(dest_key);
    dest_multi = this.multis[dest_node];
    number_keys = args[1];
    if (!dest_multi) {
      dest_multi = this.multis[dest_node] = this.redism.getClient(dest_node).multi();
    }
    _ref = args.slice(2, 2 + number_keys);
    for (_i = 0, _len = _ref.length; _i < _len; _i++) {
      arg = _ref[_i];
      node = this.redism.nodeFor(arg);
      if (remote_nodes[node] == null) {
        remote_nodes[node] = [];
      }
      remote_nodes[node].push(arg);
    }
    for (node in remote_nodes) {
      keys = remote_nodes[node];
      if (node === dest_node) {
        continue;
      }
      multi = this.multis[node];
      if (!multi) {
        multi = this.multis[node] = this.redism.getClient(node).multi();
      }
      keys.forEach((function(_this) {
        return function(key) {
          var _base, _base1;
          if ((_base = _this.temp_keys)[node] == null) {
            _base[node] = [];
          }
          _this.temp_keys[node].push(key);
          multi.migrate([dest_node.host, dest_node.port, key, dest_node.selected_db]);
          _this.interlachen.push(node);
          if ((_base1 = _this.commands)[node] == null) {
            _base1[node] = 0;
          }
          return _this.commands[node] += 1;
        };
      })(this));
    }
    dest_multi.zinterstore.call(dest_multi, args);
    this.interlachen.push(dest_node);
    if ((_base = this.commands)[dest_node] == null) {
      _base[dest_node] = 0;
    }
    return this.commands[dest_node] += 1;
  };

  Multi.prototype.exec = function(callback) {
    var nodes, self;
    nodes = Object.keys(this.multis);
    self = this;
    return step(function() {
      var group;
      group = this.group();
      return nodes.forEach(function(node) {
        return self.multis[node].exec(group());
      });
    }, function(error, groups) {
      var keys, node, results, _ref;
      _ref = self.temp_keys;
      for (node in _ref) {
        keys = _ref[node];
        self.redism[node].del(keys);
      }
      if (error) {
        return typeof callback === "function" ? callback(error) : void 0;
      }
      assert(nodes.length === groups.length, "wrong number of response");
      results = [];
      groups.forEach(function(results, index) {
        node = nodes[index];
        return assert(results.length === self.commands[node], "" + node + " is missing results");
      });
      self.interlachen.forEach(function(node) {
        var index;
        index = nodes.indexOf(node);
        return results.push(groups[index].shift());
      });
      return typeof callback === "function" ? callback(null, results) : void 0;
    });
  };

  Multi.prototype.DEL = Multi.del;

  Multi.prototype.MSET = Multi.mset;

  Multi.prototype.SINTER = Multi.siner;

  Multi.prototype.SINTERSTORE = Multi.sinterstore;

  Multi.prototype.ZINTERSTORE = Multi.zinterstore;

  Multi.prototype.EXEC = Multi.exec;

  return Multi;

})();

module.exports = Redism;
