var tape = require('tape')
var create = require('./helpers/create')
var Readable = require('stream').Readable

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

tape('basic batch', function (t) {
  t.plan(1 + 3 + 3)

  var db = create.one()

  db.batch([
    {key: 'hello', value: 'world'},
    {key: 'hej', value: 'verden'},
    {key: 'hello', value: 'welt'}
  ], function (err) {
    t.error(err, 'no error')
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hello')
      t.same(node.value, 'welt')
    })
    db.get('hej', function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, 'hej')
      t.same(node.value, 'verden')
    })
  })
})

tape('batch with del', function (t) {
  t.plan(1 + 1 + 3 + 2)

  var db = create.one()

  db.batch([
    {key: 'hello', value: 'world'},
    {key: 'hej', value: 'verden'},
    {key: 'hello', value: 'welt'}
  ], function (err) {
    t.error(err, 'no error')
    db.batch([
      {key: 'hello', value: 'verden'},
      {type: 'del', key: 'hej'}
    ], function (err) {
      t.error(err, 'no error')
      db.get('hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node.key, 'hello')
        t.same(node.value, 'verden')
      })
      db.get('hej', function (err, node) {
        t.error(err, 'no error')
        t.same(node, null)
      })
    })
  })
})
tape('multiple batches', function (t) {
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
    same('foo1', '1')
    same('foo50', '50')
    same('foo999', '999')
  })

  function same (key, val) {
    db.get(key, function (err, node) {
      t.error(err, 'no error')
      t.same(node.key, key)
      t.same(node.value, val)
    })
  }
})

tape('create with precreated keypair', function (t) {
  var crypto = require('hypercore/lib/crypto')
  var keyPair = crypto.keyPair()

  var db = create.one(keyPair.publicKey, {secretKey: keyPair.secretKey})
  db.put('hello', 'world', function (err, node) {
    t.same(node.value, 'world')
    t.error(err, 'no error')
    t.same(db.key, keyPair.publicKey, 'pubkey matches')
    db.source._storage.secretKey.read(0, keyPair.secretKey.length, function (err, secretKey) {
      t.error(err, 'no error')
      t.same(secretKey, keyPair.secretKey, 'secret key is stored')
    })
    db.get('hello', function (err, node) {
      t.error(err, 'no error')
      t.same(node.value, 'world', 'same value')
      t.end()
    })
  })
})

tape('can insert falsy values', function (t) {
  t.plan(2 * 2 + 3 + 1)

  var db = create.one(null, {valueEncoding: 'json'})

  db.put('hello', 0, function () {
    db.put('world', false, function () {
      db.get('hello', function (err, node) {
        t.error(err, 'no error')
        t.same(node && node.value, 0)
      })
      db.get('world', function (err, node) {
        t.error(err, 'no error')
        t.same(node && node.value, false)
      })

      var ite = db.iterator()
      var result = {}

      ite.next(function loop (err, node) {
        t.error(err, 'no error')

        if (!node) {
          t.same(result, {hello: 0, world: false})
          return
        }

        result[node.key] = node.value
        ite.next(loop)
      })
    })
  })
})

tape('can put/get a null value', function (t) {
  t.plan(3)

  var db = create.one(null, {valueEncoding: 'json'})
  db.put('some key', null, function (err) {
    t.error(err, 'no error')
    db.get('some key', function (err, node) {
      t.error(err, 'no error')
      t.same(node.value, null)
    })
  })
})
