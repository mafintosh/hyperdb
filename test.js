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

    db.get('foo', function (err, node) {
      assert.error(err)
      assert.equals(node.value, 'bar')

      assert.end()
    })
  })
})

test('put/get', function (assert) {
  var db = hyperdb([
    hypercore(ram, {valueEncoding: 'json'})
  ])

  db.put('foo', 'bar', function (err) {
    assert.error(err)

    db.get('foo', function (err, node) {
      assert.error(err)
      assert.ok(node.value, 'bar')

      db.put('foo', 'baz', function (err) {
        assert.error(err)

        db.get('foo', function (err, node) {
          assert.error(err)
          assert.equals(node.value, 'baz')
          assert.end()
        })
      })
    })
  })
})
