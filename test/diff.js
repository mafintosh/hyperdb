var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('empty diff', function (t) {
  var db = create.one()

  var expected = []

  var rs = db.createDiffStream('/a')
  collect(rs, function (err, actual) {
    t.error(err, 'no error')
    t.deepEqual(actual, expected, 'diff as expected')
    t.end()
  })
})

tape('implicit checkout', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a', value: '2' }
  ]

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createDiffStream('/a')
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.deepEqual(actual, expected, 'diff as expected')
      t.end()
    })
  })
})

tape('new value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a', value: '2' }
  ]

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a', '2', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream('/a', co)
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.deepEqual(actual, expected, 'diff as expected')
        t.end()
      })
    })
  })
})

tape('two new values', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a/bar', value: 'baz' },
    { type: 'put', name: '/a/foo', value: 'quux' }
  ]

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a/foo', 'quux', function (err) {
      t.error(err, 'no error')
      db.put('/a/bar', 'baz', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.deepEqual(actual, expected, 'diff as expected')
          t.end()
        })
      })
    })
  })
})

tape('opts.head', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a/foo', value: 'quux' }
  ]

  db.snapshot(function (err, co1) {
    t.error(err, 'no error')
    db.put('/a/foo', 'quux', function (err) {
      t.error(err, 'no error')
      db.snapshot(function (err, co2) {
        t.error(err, 'no error')
        db.put('/a/bar', 'baz', function (err) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/a', co1, { head: co2 })
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.deepEqual(actual, expected, 'diff as expected')
            t.end()
          })
        })
      })
    })
  })
})

tape('opts.head 2', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a/bar', value: 'baz' }
  ]

  db.put('/a/foo', 'quux', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co1) {
      t.error(err, 'no error')
      db.put('/a/bar', 'baz', function (err) {
        t.error(err, 'no error')
        db.snapshot(function (err, co2) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/a', co1, { head: co2 })
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.deepEqual(actual, expected, 'diff as expected')
            t.end()
          })
        })
      })
    })
  })
})

tape('checkout === head', function (t) {
  var db = create.one()

  var expected = [
  ]

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      var rs = db.createDiffStream('/a', co)
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.deepEqual(actual, expected, 'diff as expected')
        t.end()
      })
    })
  })
})

tape('new value, twice', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a', value: '2' }
  ]

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a', '1', function (err) {
      t.error(err, 'no error')
      db.put('/a', '2', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.deepEqual(actual, expected, 'diff as expected')
          t.end()
        })
      })
    })
  })
})

tape('untracked value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'del', name: '/a', value: '1' },
    { type: 'put', name: '/a', value: '2' }
  ]

  db.put('/a', '1', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      db.put('/a', '2', function (err) {
        t.error(err, 'no error')
        db.put('/b', '17', function (err) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/a', co)
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.deepEqual(actual, expected, 'diff as expected')
            t.end()
          })
        })
      })
    })
  })
})

tape('diff root', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/b', value: '17' },
    { type: 'del', name: '/a', value: '1' },
    { type: 'put', name: '/a', value: '2' }
  ]

  db.put('/a', '1', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      db.put('/a', '2', function (err) {
        t.error(err, 'no error')
        db.put('/b', '17', function (err) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/', co)
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.deepEqual(actual, expected, 'diff as expected')
            t.end()
          })
        })
      })
    })
  })
})

tape('updated value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'del', name: '/a/d/r', value: '1' },
    { type: 'put', name: '/a/d/r', value: '3' }
  ]

  db.put('/a/d/r', '1', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      db.put('/a/d/r', '3', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.deepEqual(actual, expected, 'diff as expected')
          t.end()
        })
      })
    })
  })
})

tape('basic with 2 feeds', function (t) {
  var expected = [
    { type: 'put', name: '/a', value: 'a' }
  ]

  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      replicate(a, b, validate)
    })

    function validate () {
      var rs = b.createDiffStream('/a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.deepEqual(actual, expected, 'diff as expected')
        t.end()
      })
    }
  })
})

function collect (stream, cb) {
  var res = []
  stream.on('data', res.push.bind(res))
  stream.once('error', cb)
  stream.once('end', cb.bind(null, null, res))
}
