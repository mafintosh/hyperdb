var tape = require('tape')
var cmp = require('compare')
var collect = require('stream-collector')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')
var put = require('./helpers/put')

tape('empty diff', function (t) {
  var db = create.one()

  var rs = db.createDiffStream(null, 'a')
  collect(rs, function (err, actual) {
    t.error(err, 'no error')
    t.deepEqual(actual, [], 'diff as expected')
    t.end()
  })
})

tape('implicit checkout', function (t) {
  var db = create.one()

  db.put('a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createDiffStream(null, 'a')
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 1)
      // t.equals(actual[0].type, 'put')
      t.equals(actual[0].left.key, 'a')
      t.equals(actual[0].left.value, '2')
      t.equals(actual[0].right, null)
      t.end()
    })
  })
})

tape('new value', function (t) {
  var db = create.one()

  db.put('a', '1', function (err) {
    t.error(err, 'no error')
    db.put('a', '2', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(null, 'a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        // t.equals(actual[0].type, 'put')
        t.equals(actual[0].left.key, 'a')
        t.equals(actual[0].left.value, '2')
        t.equals(actual[0].right, null)
        t.end()
      })
    })
  })
})

tape('two new nodes', function (t) {
  var db = create.one()

  db.put('a/foo', 'quux', function (err) {
    t.error(err, 'no error')
    db.put('a/bar', 'baz', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(null, 'a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        actual.sort(sort)
        t.equals(actual.length, 2)
        // t.equals(actual[0].type, 'put')
        t.equals(actual[0].left.key, 'a/bar')
        t.equals(actual[0].left.value, 'baz')
        t.equals(actual[0].right, null)
        // t.equals(actual[1].type, 'put')
        t.equals(actual[1].left.key, 'a/foo')
        t.equals(actual[1].left.value, 'quux')
        t.equals(actual[1].right, null)
        t.end()
      })
    })
  })
})

tape('checkout === head', function (t) {
  var db = create.one()

  db.put('a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createDiffStream(db, 'a')
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 0)
      t.end()
    })
  })
})

tape('new value, twice', function (t) {
  var db = create.one()
  var snap = db.snapshot()

  db.put('/a', '1', function (err) {
    t.error(err, 'no error')
    db.put('/a', '2', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(snap, 'a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].left.key, 'a')
        t.equals(actual[0].left.value, '2')
        t.equals(actual[0].right, null)
        t.end()
      })
    })
  })
})

tape('untracked value', function (t) {
  var db = create.one()

  db.put('a', '1', function (err) {
    t.error(err, 'no error')
    var snap = db.snapshot()
    db.put('a', '2', function (err) {
      t.error(err, 'no error')
      db.put('b', '17', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream(snap, 'a')
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 1)
          t.equals(actual[0].left.key, 'a')
          t.equals(actual[0].left.value, '2')
          t.equals(actual[0].right.key, 'a')
          t.equals(actual[0].right.value, '1')
          t.end()
        })
      })
    })
  })
})

tape('diff root', function (t) {
  var db = create.one()

  db.put('a', '1', function (err) {
    t.error(err, 'no error')
    var snap = db.snapshot()
    db.put('a', '2', function (err) {
      t.error(err, 'no error')
      db.put('b', '17', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream(snap)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          actual.sort(sort)
          t.equals(actual.length, 2)
          t.equals(actual[0].left.key, 'a')
          t.equals(actual[0].left.value, '2')
          t.equals(actual[0].right.key, 'a')
          t.equals(actual[0].right.value, '1')
          t.equals(actual[1].left.key, 'b')
          t.equals(actual[1].left.value, '17')
          t.equals(actual[1].right, null)
          t.end()
        })
      })
    })
  })
})

tape('updated value', function (t) {
  var db = create.one()

  db.put('a/d/r', '1', function (err) {
    t.error(err, 'no error')
    var snap = db.snapshot()
    db.put('a/d/r', '3', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(snap, 'a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].left.key, 'a/d/r')
        t.equals(actual[0].left.value, '3')
        t.equals(actual[0].right.key, 'a/d/r')
        t.equals(actual[0].right.value, '1')
        t.end()
      })
    })
  })
})

tape('basic with 2 feeds', function (t) {
  create.two(function (a, b) {
    a.put('a', 'a', function () {
      replicate(a, b, validate)
    })

    function validate () {
      var rs = b.createDiffStream(null, 'a', {reduce: (a, b) => a})
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].left.key, 'a')
        t.equals(actual[0].left.value, 'a')
        t.end()
      })
    }
  })
})

tape('two feeds /w competing for a value', function (t) {
  create.two(function (a, b) {
    a.put('a', 'a', function () {
      b.put('a', 'b', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createDiffStream(null, 'a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        actual[0].left.sort(sortByValue)
        t.equals(actual[0].left[0].key, 'a')
        t.equals(actual[0].left[0].value, 'a')
        t.equals(actual[0].left[1].key, 'a')
        t.equals(actual[0].left[1].value, 'b')
        t.end()
      })
    }
  })
})

tape('small diff on big db', function (t) {
  var db = create.one()

  put(db, range(1000), function (err) {
    t.error(err, 'no error')
    var snap = db.snapshot()
    db.put('42', '42*', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(snap)
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].left.key, '42')
        t.equals(actual[0].left.value, '42*')
        t.equals(actual[0].right.key, '42')
        t.equals(actual[0].right.value, '42')
        t.end()
      })
    })
  })
})

function range (n) {
  return Array(n).join('.').split('.').map((_, i) => '' + i)
}

function sortByValue (a, b) {
  return cmp(a.value, b.value)
}

function sort (a, b) {
  var ak = (a.left || a.right).key
  var bk = (b.left || b.right).key
  return cmp(ak, bk)
}
