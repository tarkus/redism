Redism = require '../lib'

servers = []
new_servers = []

for i in [0...4096]
  servers.push "redis://localhost:6379/#{i}"

new_cluster =
  'localhost:16379': [0, 1024]
  'localhost:16479': [1024, 2048]
  'localhost:16579': [2048, 3072]
  'localhost:16679': [3072, 4096]

for server, shards of new_cluster
  [start, end] = shards
  for db in [start...end]
    new_servers.push "redis://#{server}/#{db}"

describe 'Sharding', ->

  beforeEach (done) ->
    @redism = new Redism
      servers: servers

    @new_redism = new Redism
      servers: new_servers

    done()

  it 'keys should be split into different shards', (done) ->
    @redism.nodeFor('foo').should.not.equal(@redism.nodeFor('bar'))
    done()

  it 'should locate the key without problem if server config changed', (done) ->
    old_nodes =
      "redism:hash:User:8118": @redism.nodeFor('redism:hash:User:8118')
      "redism:hash:User:8117": @redism.nodeFor('redism:hash:User:8117')
      "redism:hash:User:8116": @redism.nodeFor('redism:hash:User:8116')
      "redism:hash:User:8115": @redism.nodeFor('redism:hash:User:8115')
      "redism:hash:User:8114": @redism.nodeFor('redism:hash:User:8114')
      "redism:hash:User:8113": @redism.nodeFor('redism:hash:User:8113')

    new_nodes =
      "redism:hash:User:8118": @new_redism.nodeFor('redism:hash:User:8118')
      "redism:hash:User:8117": @new_redism.nodeFor('redism:hash:User:8117')
      "redism:hash:User:8116": @new_redism.nodeFor('redism:hash:User:8116')
      "redism:hash:User:8115": @new_redism.nodeFor('redism:hash:User:8115')
      "redism:hash:User:8114": @new_redism.nodeFor('redism:hash:User:8114')
      "redism:hash:User:8113": @new_redism.nodeFor('redism:hash:User:8113')

    for key, old_node of old_nodes
      # Get old shard
      shard = parseInt(old_node.split("/").pop())

      # Get new node
      server = Math.floor(shard / 1024)
      new_node = "redis://#{Object.keys(new_cluster)[server]}/#{shard}"

      new_node.should.equal new_nodes[key]


    done()

