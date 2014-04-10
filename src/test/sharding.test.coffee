Redison = require '../lib'

describe 'Sharding', ->

  beforeEach (done) ->
    @redison = new Redison
      servers: ['localhost:6379', 'localhost:6479']
    done()

  it 'keys should be split into different shards', (done) ->
    @redison.nodeFor('foo').should.not.equal(@redison.nodeFor('bar'))
    @redison.set 'foo', 'bar'
    @redison.set 'bar', 'foo'
    done()

