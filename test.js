var test = require('tape')
var wwdb = require('.')
var hypercore = require('hypercore')
var ram = require('random-access-memory')

test('init', function (assert) {
  var db = wwdb([
    hypercore(ram)
  ])

  assert.ok(db)
  assert.end()
})
