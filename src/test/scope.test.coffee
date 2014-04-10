Redison = require '../lib'

describe 'Scope', ->

  it 'should use trigger an error if no default server set', (done) ->
    try
      redison = new Redison
        servers: [
          [ ':hash:', ['localhost:6379', 'localhost:6479'] ]
        ]
    catch e
      done()

  it 'should use correct shards for scoped key', (done) ->
    redison = new Redison
      servers: [
        [ ':hash:', ['localhost:6379', 'localhost:6479'] ]
        ['localhost:6579']
      ]

    redison.nodeFor('foo:hash:bar').should.not.equal 'localhost:6579'
    redison.nodeFor('foo1:hash:bar').should.not.equal 'localhost:6579'
    redison.nodeFor('foo12:hash:bar').should.not.equal 'localhost:6579'
    redison.nodeFor('foo123:hash:bar').should.not.equal 'localhost:6579'
    redison.nodeFor('foo1234:hash:bar').should.not.equal 'localhost:6579'

    redison.nodeFor('foo1').should.equal 'localhost:6579'
    multi = redison.multi()
    multi.sadd "foo1", 'bar'
    multi.sadd "foo1", 'bar1'
    multi.exec()
    done()
