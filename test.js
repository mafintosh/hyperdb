var test = require('tape')
var hyperdb = require('.')
var hypercore = require('hypercore')
var ram = require('random-access-memory')
var allocUnsafe = require('buffer-alloc-unsafe')
var series = require('run-series')

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

    db.get('foo', function (err, nodes) {
      assert.error(err)
      assert.equals(nodes[0].value, 'bar')

      assert.end()
    })
  })
})

test('put/get x2', function (assert) {
  var db = hyperdb([
    hypercore(ram, {valueEncoding: 'json'})
  ])

  db.put('foo', 'bar', function (err) {
    assert.error(err)

    db.get('foo', function (err, nodes) {
      assert.error(err)
      assert.ok(nodes[0].value, 'bar')

      db.put('foo', 'baz', function (err) {
        assert.error(err)

        db.get('foo', function (err, nodes) {
          assert.error(err)
          assert.equals(nodes[0].value, 'baz')
          assert.end()
        })
      })
    })
  })
})

test('writable', function (assert) {
  var db = hyperdb([
    hypercore(ram, {valueEncoding: 'json'})
  ])

  assert.equals(db.writable, false)
  db.ready(function (err) {
    assert.error(err)
    assert.equals(db.writable, true)
    assert.end()
  })
})

test('readable', function (assert) {
  var db = hyperdb([
    hypercore(ram, {valueEncoding: 'json'})
  ])

  assert.equals(db.readable, true)
  db.close(function (err) {
    assert.error(err)
    assert.equals(db.writable, false)
    assert.equals(db.readable, false)
    assert.end()
  })
})

test('read-only', function (assert) {
  var a = hypercore(ram, allocUnsafe(32), {valueEncoding: 'json'})
  var b = hypercore(ram, allocUnsafe(32), {valueEncoding: 'json'})

  var db = hyperdb([a, b])

  db.ready(function (err) {
    assert.error(err)
    assert.equals(db.writable, false)
    assert.end()
  })
})

test('example', function (assert) {
  var log1write = hypercore(ram, {valueEncoding: 'json'})
  var log2write = hypercore(ram, {valueEncoding: 'json'})

  series([
    log1write.ready.bind(log1write),
    log2write.ready.bind(log2write)
  ], function (err) {
    assert.error(err)

    var log1read = hypercore(ram, log1write.key, {valueEncoding: 'json'})
    var log2read = hypercore(ram, log2write.key, {valueEncoding: 'json'})

    var db1 = hyperdb([
      log1write, log2read
    ])

    var db2 = hyperdb([
      log1read, log2write
    ])

    var destroy1 = replicate(log1write, log1read)
    var destroy2 = replicate(log2write, log2read)

    series([
      putGet('/a', ['1.0'], db1, db2), // add first key
      putGet('/a', ['2.0'], db2, db1), // overwrite key causally dependent on db1
      putGet('/b/c', ['1.1'], db1, db2), // add new nested key
      putGet('/b/d', ['1.2'], db1, db2), // add another nested key
      putGet('/a', ['1.3'], db1, db2), // overwrite key from db2
      function (next) { // net split
        destroy1()
        destroy2()
        setImmediate(next)
      },
      put('/c', '1.4', db1), // write key to one side
      put('/c', '2.1', db2), // write same key to other side
      function (next) { // restart replication
        destroy1 = replicate(log1write, log1read)
        destroy2 = replicate(log2write, log2read)
        setImmediate(next)
      },
      get('/c', ['1.4', '2.1'], db1), // return conflicting values
      get('/c', ['1.4', '2.1'], db2), // return conflicting values on other side too
      putGet('/d', ['1.5'], db1, db2), // add new key while another key is in conflict
      put('/c', '1.6', db1), // "resolve" conflict
      get('/c', ['1.6'], db2), // resolved value
      get('/c', ['1.6'], db1) // resolved value other side
    ], function (err) { // end
      destroy1()
      destroy2()
      assert.end(err)
    })
  })

  function put (key, value, db) {
    return function (cb) {
      db.put(key, value, function (err) {
        if (err) return cb(err)
        setImmediate(cb) // wait for in-memory hypercore's to sync
      })
    }
  }
  function get (key, values, db) {
    return function (cb) {
      db.get(key, function (err, nodes) {
        if (err) return cb(err)
        assert.deepEquals(nodes.map(function (n) { return n.value }).sort(), values)
        setImmediate(cb) // wait for in-memory hypercore's to sync
      })
    }
  }

  function putGet (key, values, writeDb, readDb) {
    return function (cb) {
      put(key, values[0], writeDb)(function (err) {
        if (err) return cb(err)
        get(key, values, readDb)(cb)
      })
    }
  }
})

function replicate (a, b) {
  var s1 = a.replicate({live: true})
  var s2 = b.replicate({live: true})

  s1.pipe(s2).pipe(s1)

  var called = false
  return function () {
    if (called === true) return
    s1.destroy()
    s2.destroy()
    called = true
  }
}
