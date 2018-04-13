var tape = require('tape')
var create = require('./helpers/create')

tape('two keys with same siphash', function (t) {
  t.plan(2 + 2)

  var db = create.one()

  db.put('idgcmnmna', 'a', function () {
    db.put('mpomeiehc', 'b', function () {
      db.get('idgcmnmna', function (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'a')
      })
      db.get('mpomeiehc', function (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'b')
      })
    })
  })
})

tape('two keys with same siphash (iterator)', function (t) {
  var db = create.one()

  db.put('idgcmnmna', 'a', function () {
    db.put('mpomeiehc', 'b', function () {
      var ite = db.iterator()

      ite.next(function (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'a')
      })
      ite.next(function (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'b')
      })
      ite.next(function (err, node) {
        t.error(err, 'no error')
        t.same(node, null)
        t.end()
      })
    })
  })
})

tape('two prefixes with same siphash (iterator)', function (t) {
  var db = create.one()

  db.put('idgcmnmna/a', 'a', function () {
    db.put('mpomeiehc/b', 'b', function () {
      var ite = db.iterator('idgcmnmna')

      ite.next(function (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'a')
      })
      ite.next(function (err, node) {
        t.error(err, 'no error')
        t.same(node, null)
        t.end()
      })
    })
  })
})
