var tape = require('tape')
var cmp = require('compare')
var create = require('./helpers/create')
var run = require('./helpers/run')
var replicate = require('./helpers/replicate')

tape('two writers, no conflicts, many values', function (t) {
  t.plan(1 + 3 * 4)

  create.two(function (db1, db2, replicate) {
    var r = []
    for (var i = 0; i < 1000; i++) r.push('i' + i)

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

tape('two writers, one conflict', function (t) {
  t.plan(1 + 4 * 2 + 6 * 2)
  create.two(function (db1, db2, replicate) {
    run(
      cb => db1.put('a', 'a', cb),
      cb => replicate(cb),
      cb => db1.put('b', 'b', cb),
      cb => db2.put('b', 'B', cb),
      cb => replicate(cb),
      cb => db1.put('a', 'A', cb),
      cb => replicate(cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')

      db1.get('a', ona)
      db2.get('a', ona)
      db1.get('b', onb)
      db2.get('b', onb)

      function onb (err, nodes) {
        t.error(err, 'no error')
        nodes.sort((a, b) => cmp(a.value, b.value))
        t.same(nodes.length, 2)
        t.same(nodes[0].key, 'b')
        t.same(nodes[0].value, 'B')
        t.same(nodes[1].key, 'b')
        t.same(nodes[1].value, 'b')
      }

      function ona (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes.length, 1)
        t.same(nodes[0].key, 'a')
        t.same(nodes[0].value, 'A')
      }
    }
  })
})

tape('two writers, fork', function (t) {
  t.plan(4 * 2 + 1)

  create.two(function (a, b, replicate) {
    run(
      cb => a.put('a', 'a', cb),
      replicate,
      cb => b.put('a', 'b', cb),
      cb => a.put('b', 'c', cb),
      replicate,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      a.get('a', ona)
      b.get('a', ona)
    }

    function ona (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 1)
      t.same(nodes[0].key, 'a')
      t.same(nodes[0].value, 'b')
    }
  })
})

tape('three writers, two forks', function (t) {
  t.plan(4 * 3 + 1)

  create.three(function (a, b, c, replicateAll) {
    run(
      cb => a.put('a', 'a', cb),
      replicateAll,
      cb => b.put('a', 'ab', cb),
      cb => a.put('some', 'some', cb),
      cb => replicate(a, c, cb),
      cb => c.put('c', 'c', cb),
      replicateAll,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      a.get('a', ona)
      b.get('a', ona)
      c.get('a', ona)

      function ona (err, nodes) {
        t.error(err, 'no error')
        t.same(nodes.length, 1, 'one node')
        t.same(nodes[0].key, 'a')
        t.same(nodes[0].value, 'ab')
      }
    }
  })
})

tape('two writers, simple fork', function (t) {
  t.plan(1 + 2 * (4 + 6) + 2 + 4)
  create.two(function (db1, db2, replicate) {
    run(
      cb => db1.put('0', '0', cb),
      replicate,
      cb => db1.put('1', '1a', cb),
      cb => db2.put('1', '1b', cb),
      replicate,
      cb => db1.put('2', '2', cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      db1.get('0', on0)
      db1.get('1', on1)
      db1.get('2', on2db1)
      db2.get('0', on0)
      db2.get('1', on1)
      db2.get('2', on2db2)
    }

    function on0 (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 1)
      t.same(nodes[0].key, '0')
      t.same(nodes[0].value, '0')
    }

    function on1 (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 2)
      nodes.sort((a, b) => cmp(a.value, b.value))
      t.same(nodes[0].key, '1')
      t.same(nodes[0].value, '1a')
      t.same(nodes[1].key, '1')
      t.same(nodes[1].value, '1b')
    }

    function on2db1 (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 1)
      t.same(nodes[0].key, '2')
      t.same(nodes[0].value, '2')
    }

    function on2db2 (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 0)
    }
  })
})

tape('three writers, no conflicts, forks', function (t) {
  t.plan(1 + 4 * 3)

  create.three(function (a, b, c, replicateAll) {
    run(
      cb => c.put('a', 'ac', cb),
      replicateAll,
      cb => a.put('foo', 'bar', cb),
      replicateAll,
      cb => a.put('a', 'aa', cb),
      cb => replicate(a, b, cb),
      range(50).map(key => cb => b.put(key, key, cb)),
      replicateAll,
      range(5).map(key => cb => c.put(key, 'c' + key, cb)),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      a.get('a', ona)
      b.get('a', ona)
      c.get('a', ona)
    }

    function ona (err, nodes) {
      t.error(err, 'no error')
      t.same(nodes.length, 1)
      t.same(nodes[0].key, 'a')
      t.same(nodes[0].value, 'aa')
    }
  })
})

tape('replication to two new peers, only authorize one writer', function (t) {
  var a = create.one()
  a.ready(function () {
    var b = create.one(a.key)
    var c = create.one(a.key)

    run(
      cb => b.ready(cb),
      cb => c.ready(cb),
      cb => a.put('foo', 'bar', cb),
      cb => a.authorize(b.local.key, cb),
      cb => replicate(a, b, cb),
      cb => replicate(a, c, cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      c.authorized(c.local.key, function (err, auth) {
        t.error(err, 'no error')
        t.notOk(auth)
        t.end()
      })
    }
  })
})

tape('2 unauthed clones', function (t) {
  t.plan(1 + 2 * 2)

  var db = create.one(null)

  db.ready(function () {
    var clone1 = create.one(db.key)
    var clone2 = create.one(db.key)

    run(
      cb => db.put('hello', 'world', cb),
      cb => clone1.ready(cb),
      cb => replicate(db, clone1, cb),
      cb => clone2.ready(cb),
      cb => replicate(clone1, clone2, cb),
      done
    )

    function done (err) {
      t.error(err, 'no error')
      clone1.get('hello', onhello)
      clone2.get('hello', onhello)

      function onhello (err, node) {
        t.error(err, 'no error')
        t.same(node.value, 'world')
      }
    }
  })
})

function range (n) {
  return Array(n).join(',').split(',').map((_, i) => '' + i)
}
