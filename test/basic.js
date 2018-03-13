var tape = require('tape')
var create = require('./helpers/create')

tape('basic put/get', function (t) {
  var db = create.one()
  db.put('hello', 'world', function (err, node) {
    t.same(node.key, 'hello')
    t.same(node.value, 'world')
    t.error(err, 'no error')
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello', 'same key')
      t.same(node.value, 'world', 'same value')
      t.end()
    })
  })
})

tape('get on empty db', function (t) {
  var db = create.one()

  db.get('hello', function (err, node) {
    t.error(err, 'no error')
    t.same(node, null, 'node is not found')
    t.end()
  })
})

tape('not found', function (t) {
  var db = create.one()
  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('hej', function (err, node) {
      t.error(err, 'no error')
      t.same(node, null, 'node is not found')
      t.end()
    })
  })
})

tape('leading / is ignored', function (t) {
  t.plan(7)
  var db = create.one()
  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('/hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello', 'same key')
      t.same(node.value, 'world', 'same value')
    })
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello', 'same key')
      t.same(node.value, 'world', 'same value')
    })
  })
})

tape('multiple put/get', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.put('world', 'hello', function (err) {
      t.error(err, 'no error')
      db.get('hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello', 'same key')
        t.same(node.value, 'world', 'same value')
      })
      db.get('world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'world', 'same key')
        t.same(node.value, 'hello', 'same value')
      })
    })
  })
})

tape('overwrites', function (t) {
  var db = create.one()

  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello', 'same key')
      t.same(node.value, 'world', 'same value')
      db.put('hello', 'verden', function (err) {
        t.error(err, 'no error')
        db.get('hello', function (err, node) {
          t.error(err, 'no error')
          t.same(node.key, 'hello', 'same key')
          t.same(node.value, 'verden', 'same value')
          t.end()
        })
      })
    })
  })
})

tape('put/gets namespaces', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('hello/world', 'world', function (err) {
    t.error(err, 'no error')
    db.put('world', 'hello', function (err) {
      t.error(err, 'no error')
      db.get('hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello/world', 'same key')
        t.same(node.value, 'world', 'same value')
      })
      db.get('world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'world', 'same key')
        t.same(node.value, 'hello', 'same value')
      })
    })
  })
})

tape('put in tree', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('hello', 'a', function (err) {
    t.error(err, 'no error')
    db.put('hello/world', 'b', function (err) {
      t.error(err, 'no error')
      db.get('hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello', 'same key')
        t.same(node.value, 'a', 'same value')
      })
      db.get('hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello/world', 'same key')
        t.same(node.value, 'b', 'same value')
      })
    })
  })
})

tape('put in tree reverse order', function (t) {
  t.plan(8)

  var db = create.one()

  db.put('hello/world', 'b', function (err) {
    t.error(err, 'no error')
    db.put('hello', 'a', function (err) {
      t.error(err, 'no error')
      db.get('hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello', 'same key')
        t.same(node.value, 'a', 'same value')
      })
      db.get('hello/world', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello/world', 'same key')
        t.same(node.value, 'b', 'same value')
      })
    })
  })
})

tape('multiple put in tree', function (t) {
  t.plan(13)

  var db = create.one()

  db.put('hello/world', 'b', function (err) {
    t.error(err, 'no error')
    db.put('hello', 'a', function (err) {
      t.error(err, 'no error')
      db.put('hello/verden', 'c', function (err) {
        t.error(err, 'no error')
        db.put('hello', 'd', function (err) {
          t.error(err, 'no error')
          db.get('hello', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, 'hello', 'same key')
            t.same(node.value, 'd', 'same value')
          })
          db.get('hello/world', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, 'hello/world', 'same key')
            t.same(node.value, 'b', 'same value')
          })
          db.get('hello/verden', function (err, node) {
            t.error(err, 'no error')
            t.same(node.key, 'hello/verden', 'same key')
            t.same(node.value, 'c', 'same value')
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
      t.same(node.key, key, 'same key')
      t.same(node.value, key, 'same value')
    }
  }
})

tape('race works', function (t) {
  t.plan(40)

  var missing = 10
  var db = create.one()

  for (var i = 0; i < 10; i++) db.put('#' + i, '#' + i, done)

  function done (err) {
    t.error(err, 'no error')
    if (--missing) return
    for (var i = 0; i < 10; i++) same('#' + i)
  }

  function same (val) {
    db.get(val, function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, val, 'same key')
      t.same(node.value, val, 'same value')
    })
  }
})

tape('version', function (t) {
  var db = create.one()

  db.version(function (err, version) {
    t.error(err, 'no error')
    t.same(version, Buffer.alloc(0))
    db.put('hello', 'world', function () {
      db.version(function (err, version) {
        t.error(err, 'no error')
        db.put('hello', 'verden', function () {
          db.checkout(version).get('hello', function (err, node) {
            t.error(err, 'no error')
            t.same(node.value, 'world')
            t.end()
          })
        })
      })
    })
  })
})
