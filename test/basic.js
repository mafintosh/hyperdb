var tape = require('tape')
var create = require('./helpers/create')
var Readable = require('stream').Readable

tape('basic put/get', function (t) {
  var db = create.one()
  db.put('/hello', 'world', function (err, _node) {
    t.error(err, 'no error')
    db.get('/hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, '/hello', 'same key')
      t.same(node.value, 'world', 'same value')
      t.deepEquals(node, _node)
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
  t.plan(9)

  var db = create.one()

  db.put('/hello', 'world', function (err) {
    t.error(err, 'no error')
    db.put('/world', 'hello', function (err, _node) {
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
        t.same(_node, node)
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

  var db = create.one()

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

  var db = create.one()

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

  var db = create.one()

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
      t.same(node.key, val)
      t.same(node.value, val)
    })
  }
})

tape('batch', function (t) {
  t.plan(19)

  var db = create.one()

  db.batch([{
    type: 'put',
    key: 'foo',
    value: 'foo'
  }, {
    type: 'put',
    key: 'bar',
    value: 'bar'
  }], function (err, nodes) {
    t.error(err)
    t.same(2, nodes.length)
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
    }], function (err, nodes) {
      t.error(err)
      t.same(3, nodes.length)
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

tape('createWriteStream pipe', function (t) {
  t.plan(10)
  var db = create.one()
  var writer = db.createWriteStream()
  var index = 0
  var reader = new Readable({
    objectMode: true,
    read: function (size) {
      var value = (index < 1000) ? {
        type: 'put',
        key: 'foo' + index,
        value: index++
      } : null
      this.push(value)
    }
  })
  reader.pipe(writer)
  writer.on('finish', function (err) {
    t.error(err, 'no error')
    same('foo1', 1)
    same('foo50', 50)
    same('foo999', 999)
  })

  function same (key, val) {
    db.get(key, function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, key)
      t.same(node.value, val)
    })
  }
})
