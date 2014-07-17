Redism = require '../lib'

describe 'Connection', ->

  beforeEach (done) ->
    @redism = new Redism
      name: "test"
      servers: ['redis://localhost:6379/3', 'redis://localhost:6479']
    done()

  it 'should be connected', (done) ->
    Object.keys(@redism.clients).length.should.equal 2
    setTimeout =>
      @redism.isReady().should.be.ok
      done()
    , 10

