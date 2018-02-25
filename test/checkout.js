var tape = require('tape')
var create = require('./helpers/create')

tape('basic checkout', function (t) {
  var db = create.one()
  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.version(function (err, v) {
      t.error(err, 'no error')
      var old = db.checkout(v)
      db.get('/hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello', 'same key')
        t.same(node.value, 'world', 'same value')
        db.put('/hello', 'verden', function () {
          old.get('/hello', function (err, nodes) {
            t.error(err, 'no error')
            t.same(nodes[0].value, 'world')
            db.get('/hello', function (err, node) {
              t.error(err, 'no error')
              t.same(node.value, 'verden')
              t.end()
            })
          })
        })
      })
    })
  })
})

tape('checkout gets should pass for all keys inserted before checkout seq', function (t) {
  t.plan(7)

  var db1 = create.one()
  db1.put('a', 'b', function (err) {
    t.error(err)
    db1.put('c', 'd', function (err) {
      t.error(err)
      checkoutAndTest()
    })
  })

  function checkoutAndTest () {
    db1.version(function (err, version) {
      t.error(err)
      var db2 = db1.checkout(version)
      db2.get('a', function (err, nodes) {
        t.error(err)
        t.same(nodes[0].value, 'b') // This passes.
        db2.get('c', function (err, nodes) {
          t.error(err)
          t.same(nodes[0].value, 'd') // This fails.
        })
      })
    })
  }
})
