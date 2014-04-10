Redison = require '../lib'
should  = require 'should'

describe 'Multi', ->

  beforeEach (done) ->
    @redison = new Redison
      servers: ['localhost:6379', 'localhost:6479']
    done()

  it 'should be executed in the right way', (done) ->
    multi = @redison.multi()
    multi.set 'foo', 'bar'
    multi.set 'bar', 'foo'
    multi.set 'a', 'b'
    multi.exec (error, results) ->
      should.not.exist error
      done()