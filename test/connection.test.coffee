Redism = require '../lib'

describe 'Connection', ->

  beforeEach (done) ->
    @dummy_redism = new Redism name: 'dummy'

    @on_demand_redism = new Redism
      name: "test"
      servers: ['redis://localhost:6379/3', 'redis://localhost:6479']

    @instant_redism = new Redism
      name: "test"
      servers: ['redis://localhost:6379/3', 'redis://localhost:6479']
      instant: true
    done()

  it 'should be connected', (done) ->
    @dummy_redism.options.nodes.should.eql ['redis://localhost:6379/0']
    @dummy_redism.node_list.should.eql ['redis://localhost:6379/0']
    @on_demand_redism.isReady().should.be.ok
    Object.keys(@instant_redism.clients).length.should.equal 2
    setTimeout =>
      @instant_redism.isReady().should.be.ok
      done()
    , 10

