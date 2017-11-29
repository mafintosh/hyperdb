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

