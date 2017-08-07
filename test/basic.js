var tape = require('tape')
var create = require('./helpers/create')

tape('basic put/get', function (t) {
  var db = create.one()
  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('/hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, '/hello', 'same key')
      t.same(node.value, 'world', 'same value')
      t.end()
    })
  })
})

tape('not found', function (t) {
  var db = create.one()
  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('/hej', function (err, node) {
      t.error(err, 'no error')
      t.same(node, null)
      t.end()
    })
  })
})

tape('multiple put/get', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.put('/world', 'hello', function (err) {
      t.error(err, 'no error')
      db.get('/hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello', 'same key')
        t.same(node.value, 'world', 'same value')
      })
      db.get('/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/world', 'same key')
        t.same(node.value, 'hello', 'same value')
      })
    })
  })
})

tape('overwrites', function (t) {
  var db = create.one()

  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('/hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, '/hello')
      t.same(node.value, 'world')
      db.put('/hello', 'verden', function (err) {
        t.error(err, 'no error')
        db.get('/hello', function (err, node) {
          t.error(err, 'no error')
          t.same(node.key, '/hello')
          t.same(node.value, 'verden')
          t.end()
        })
      })
    })
  })
})

tape('put/gets namespaces', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('/hello/world', 'world', function (err) {
    t.error(err, 'no error')
    db.put('/world', 'hello', function (err) {
      t.error(err, 'no error')
      db.get('/hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello/world')
        t.same(node.value, 'world')
      })
      db.get('/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/world')
        t.same(node.value, 'hello')
      })
    })
  })
})

tape('put in tree', function (t) {
  t.plan(8)

  var db  = create.one()

  db.put('/hello', 'a', function (err) {
    t.error(err, 'no error')
    db.put('/hello/world', 'b', function (err) {
      t.error(err, 'no error')
      db.get('/hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello')
        t.same(node.value, 'a')
      })
      db.get('/hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello/world')
        t.same(node.value, 'b')
      })
    })
  })
})

tape('put in tree reverse order', function (t) {
  t.plan(8)

  var db  = create.one()

  db.put('/hello/world', 'b', function (err) {
    t.error(err, 'no error')
    db.put('/hello', 'a', function (err) {
      t.error(err, 'no error')
      db.get('/hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello')
        t.same(node.value, 'a')
      })
      db.get('/hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, '/hello/world')
        t.same(node.value, 'b')
      })
    })
  })
})

tape('multiple put in tree', function (t) {
  t.plan(13)

  var db  = create.one()

  db.put('/hello/world', 'b', function (err) {
    t.error(err, 'no error')
    db.put('/hello', 'a', function (err) {
      t.error(err, 'no error')
      db.put('/hello/verden', 'c', function (err) {
        t.error(err, 'no error')
        db.put('/hello', 'd', function (err) {
          t.error(err, 'no error')
          db.get('/hello', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, '/hello')
            t.same(node.value, 'd')
          })
          db.get('/hello/world', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, '/hello/world')
            t.same(node.value, 'b')
          })
          db.get('/hello/verden', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, '/hello/verden')
            t.same(node.value, 'c')
          })
        })
      })
    })
  })
})

tape('insert 100 values and get them all', function (t) {
  var db = create.one()
  var max = 100
  var i = 0

  t.plan(3 * max)

  loop()

  function loop () {
    if (i === max) return validate()
    db.put('#' + i, '#' + (i++), loop)
  }

  function validate () {
    for (var i = 0; i < max; i++) {
      db.get('#' + i, same('#' + i))
    }
  }

  function same (key) {
    return function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, key)
      t.same(node.value, key)
    }
  }
})
