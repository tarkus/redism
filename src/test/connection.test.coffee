Redison = require '../lib'

describe 'Connection', ->

  beforeEach (done) ->
    @redison = new Redison
      servers: ['localhost:6379', 'localhost:6479']
    done()

  it 'should be connected', (done) ->
    @redison.servers.length.should.equal 2
    done()

