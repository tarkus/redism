Redison = require '../lib'

describe 'Sharding', ->

  beforeEach (done) ->
    @redison = new Redison
      servers: ['redis://localhost:6379', 'redis://localhost:6479']
    done()

  it 'keys should be split into different shards', (done) ->
    @redison.nodeFor('foo').should.not.equal(@redison.nodeFor('bar'))
    @redison.set 'foo', 'bar'
    @redison.set 'bar', 'foo'
    done()

