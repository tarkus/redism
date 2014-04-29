Redism = require '../lib'

describe 'Scope', ->

  it 'should use correct shards for scoped key', (done) ->
    redism = new Redism
      servers: [
        [ ':hash:', ['redis://localhost:6379'] ]
        ['redis://localhost:6479']
      ]

    redism.nodeFor('foo:hash:bar').should.not.equal 'redis://localhost:6479'
    redism.nodeFor('foo1:hash:bar').should.not.equal 'redis://localhost:6479'
    redism.nodeFor('foo12:hash:bar').should.not.equal 'redis://localhost:6479'
    redism.nodeFor('foo123:hash:bar').should.not.equal 'redis://localhost:6479'
    redism.nodeFor('foo1234:hash:bar').should.not.equal 'redis://localhost:6479'

    redism.nodeFor('foo1').should.equal 'redis://localhost:6479'
    multi = redism.multi()
    multi.sadd "foo1", 'bar'
    multi.sadd "foo1", 'bar1'
    multi.exec()
    done()
