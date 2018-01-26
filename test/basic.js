var tape = require('tape')
var create = require('./helpers/create')
var run = require('./helpers/run')

tape('basic put/get', function (t) {
  var db = create.one()
  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello', 'same key')
      t.same(node.value, 'world', 'same value')
      t.end()
    })
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

tape('two writers, no conflicts, many values', function (t) {
  t.plan(1 + 3 * 4)

  create.two(function (db1, db2, replicate) {
    var r = []
    for (var i = 0; i < 10000; i++) r.push('i' + i)

    run(
      cb => db1.put('0', '0', cb),
      cb => replicate(cb),
      cb => db2.put('a', 'a', cb),
      cb => replicate(cb),
      cb => db2.put('2', '2', cb),
      cb => db2.put('3', '3', cb),
      cb => db2.put('4', '4', cb),
      cb => db2.put('5', '5', cb),
      cb => db2.put('6', '6', cb),
      cb => db2.put('7', '7', cb),
      cb => db2.put('8', '8', cb),
      cb => db2.put('9', '9', cb),
      cb => replicate(cb),
      cb => db1.put('b', 'b', cb),
      cb => db2.put('c', 'c', cb),
      cb => replicate(cb),
      cb => db2.put('d', 'd', cb),
      cb => replicate(cb),
      r.map(i => cb => db1.put(i, i, cb)),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      db2.get('a', expect('a'))
      db1.get('0', expect('0'))
      db1.get('i424', expect('i424'))

      function expect (v) {
        return function (err, nodes) {
          t.error(err, 'no error')
          t.same(nodes.length, 1)
          t.same(nodes[0].key, v)
          t.same(nodes[0].value, v)
        }
      }
    }
  })
})

/*
tape('batch', function (t) {
  t.plan(17)

  var db = create.one()

  db.batch([{
    type: 'put',
    key: 'foo',
    value: 'foo'
  }, {
    type: 'put',
    key: 'bar',
    value: 'bar'
  }], function (err) {
    t.error(err)
    same('foo', 'foo')
    same('bar', 'bar')
    db.batch([{
      type: 'put',
      key: 'foo',
      value: 'foo2'
    }, {
      type: 'put',
      key: 'bar',
      value: 'bar2'
    }, {
      type: 'put',
      key: 'baz',
      value: 'baz'
    }], function (err) {
      t.error(err)
      same('foo', 'foo2')
      same('bar', 'bar2')
      same('baz', 'baz')
    })
  })

  function same (key, val) {
    db.get(key, function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, key)
      t.same(node.value, val)
    })
  }
})

tape('createWriteStream', function (t) {
  t.plan(10)
  var db = create.one()
  var writer = db.createWriteStream()

  writer.write([{
    type: 'put',
    key: 'foo',
    value: 'foo'
  }, {
    type: 'put',
    key: 'bar',
    value: 'bar'
  }])

  writer.write({
    type: 'put',
    key: 'baz',
    value: 'baz'
  })

  writer.end(function (err) {
    t.error(err, 'no error')
    same('foo', 'foo')
    same('bar', 'bar')
    same('baz', 'baz')
  })

  function same (key, val) {
    db.get(key, function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, key)
      t.same(node.value, val)
    })
  }
})
*/
