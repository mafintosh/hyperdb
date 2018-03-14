var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('basic watch', function (t) {
  var db = create.one()

  db.watch(function () {
    t.pass('watch triggered')
    t.end()
  })

  db.put('hello', 'world')
})

tape('watch prefix', function (t) {
  var db = create.one()
  var changed = false

  db.watch('foo', function () {
    t.ok(changed)
    t.end()
  })

  db.put('hello', 'world', function (err) {
    t.error(err)
    setImmediate(function () {
      changed = true
      db.put('foo/bar', 'baz')
    })
  })
})

tape('remote watch', function (t) {
  var db = create.one()

  db.ready(function () {
    var clone = create.one(db.key)

    for (var i = 0; i < 100; i++) db.put('hello-' + i, 'world-' + i)
    db.put('flush', 'flush', function () {
      clone.watch(function () {
        t.pass('remote watch triggered')
        t.end()
      })

      replicate(db, clone)
    })
  })
})
