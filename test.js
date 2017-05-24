var test = require('tape')
var hyperdb = require('.')
var hypercore = require('hypercore')
var ram = require('random-access-memory')

test('init', function (assert) {
  var db = hyperdb([
    hypercore(ram)
  ])

  assert.ok(db)
  assert.end()
})

test('put/get', function (assert) {
  var db = hyperdb([
    hypercore(ram, {valueEncoding: 'json'})
  ])

  db.put('foo', 'bar', function (err) {
    assert.error(err)

    db.get('foo', function (err, val) {
      assert.error(err)
      assert.ok(val)

      assert.end()
    })
  })
})
