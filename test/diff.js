var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('empty diff', function (t) {
  var db = create.one()

  var rs = db.createDiffStream('/a')
  collect(rs, function (err, actual) {
    t.error(err, 'no error')
    t.deepEqual(actual, [], 'diff as expected')
    t.end()
  })
})

tape('implicit checkout', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createDiffStream('/a')
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 1)
      t.equals(actual[0].type, 'put')
      t.equals(actual[0].name, '/a')
      t.equals(actual[0].nodes.length, 1)
      t.equals(actual[0].nodes[0].value, '2')
      t.end()
    })
  })
})

tape('new value', function (t) {
  var db = create.one()

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a', '2', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream('/a', co)
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].type, 'put')
        t.equals(actual[0].name, '/a')
        t.equals(actual[0].nodes.length, 1)
        t.equals(actual[0].nodes[0].value, '2')
        t.end()
      })
    })
  })
})

tape('two new nodes', function (t) {
  var db = create.one()

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a/foo', 'quux', function (err) {
      t.error(err, 'no error')
      db.put('/a/bar', 'baz', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 2)
          t.equals(actual[0].type, 'put')
          t.equals(actual[0].name, '/a/bar')
          t.equals(actual[0].nodes.length, 1)
          t.equals(actual[0].nodes[0].value, 'baz')
          t.equals(actual[1].type, 'put')
          t.equals(actual[1].name, '/a/foo')
          t.equals(actual[1].nodes.length, 1)
          t.equals(actual[1].nodes[0].value, 'quux')
          t.end()
        })
      })
    })
  })
})

tape('set head', function (t) {
  var db = create.one()

  db.snapshot(function (err, co1) {
    t.error(err, 'no error')
    db.put('/a/foo', 'quux', function (err) {
      t.error(err, 'no error')
      db.snapshot(function (err, co2) {
        t.error(err, 'no error')
        db.put('/a/bar', 'baz', function (err) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/a', co1, co2)
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.equals(actual.length, 1)
            t.equals(actual[0].type, 'put')
            t.equals(actual[0].name, '/a/foo')
            t.equals(actual[0].nodes.length, 1)
            t.equals(actual[0].nodes[0].value, 'quux')
            t.end()
          })
        })
      })
    })
  })
})

tape('set head 2', function (t) {
  var db = create.one()

  db.put('/a/foo', 'quux', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co1) {
      t.error(err, 'no error')
      db.put('/a/bar', 'baz', function (err) {
        t.error(err, 'no error')
        db.snapshot(function (err, co2) {
          t.error(err, 'no error')
          var rs = db.createDiffStream('/a', co1, co2)
          collect(rs, function (err, actual) {
            t.error(err, 'no error')
            t.equals(actual.length, 1)
            t.equals(actual[0].type, 'put')
            t.equals(actual[0].name, '/a/bar')
            t.equals(actual[0].nodes.length, 1)
            t.equals(actual[0].nodes[0].value, 'baz')
            t.end()
          })
        })
      })
    })
  })
})

tape('checkout === head', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      var rs = db.createDiffStream('/a', co)
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 0)
        t.end()
      })
    })
  })
})

tape('new value, twice', function (t) {
  var db = create.one()

  db.snapshot(function (err, co) {
    t.error(err, 'no error')
    db.put('/a', '1', function (err) {
      t.error(err, 'no error')
      db.put('/a', '2', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 1)
          t.equals(actual[0].type, 'put')
          t.equals(actual[0].name, '/a')
          t.equals(actual[0].nodes.length, 1)
          t.equals(actual[0].nodes[0].value, '2')
          t.end()
        })
      })
    })
  })
})

tape('untracked value', function (t) {
  var db = create.one()

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
            t.equals(actual.length, 2)
            t.equals(actual[0].type, 'del')
            t.equals(actual[0].name, '/a')
            t.equals(actual[0].nodes.length, 1)
            t.equals(actual[0].nodes[0].value, '1')
            t.equals(actual[1].type, 'put')
            t.equals(actual[1].name, '/a')
            t.equals(actual[1].nodes.length, 1)
            t.equals(actual[1].nodes[0].value, '2')
            t.end()
          })
        })
      })
    })
  })
})

tape('diff root', function (t) {
  var db = create.one()

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
            t.equals(actual.length, 3)
            t.equals(actual[0].type, 'put')
            t.equals(actual[0].name, '/b')
            t.equals(actual[0].nodes.length, 1)
            t.equals(actual[0].nodes[0].value, '17')
            t.equals(actual[1].type, 'del')
            t.equals(actual[1].name, '/a')
            t.equals(actual[1].nodes.length, 1)
            t.equals(actual[1].nodes[0].value, '1')
            t.equals(actual[2].type, 'put')
            t.equals(actual[2].name, '/a')
            t.equals(actual[2].nodes.length, 1)
            t.equals(actual[2].nodes[0].value, '2')
            t.end()
          })
        })
      })
    })
  })
})

tape('updated value', function (t) {
  var db = create.one()

  db.put('/a/d/r', '1', function (err) {
    t.error(err, 'no error')
    db.snapshot(function (err, co) {
      t.error(err, 'no error')
      db.put('/a/d/r', '3', function (err) {
        t.error(err, 'no error')
        var rs = db.createDiffStream('/a', co)
        collect(rs, function (err, actual) {
          t.error(err, 'no error')
          t.equals(actual.length, 2)
          t.equals(actual[0].type, 'del')
          t.equals(actual[0].name, '/a/d/r')
          t.equals(actual[0].nodes.length, 1)
          t.equals(actual[0].nodes[0].value, '1')
          t.equals(actual[1].type, 'put')
          t.equals(actual[1].name, '/a/d/r')
          t.equals(actual[1].nodes.length, 1)
          t.equals(actual[1].nodes[0].value, '3')
          t.end()
        })
      })
    })
  })
})

tape('basic with 2 feeds', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      replicate(a, b, validate)
    })

    function validate () {
      var rs = b.createDiffStream('/a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].type, 'put')
        t.equals(actual[0].name, '/a')
        t.equals(actual[0].nodes.length, 1)
        t.equals(actual[0].nodes[0].value, 'a')
        t.end()
      })
    }
  })
})

tape('two feeds /w competing for a value', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createDiffStream('/a')
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 1)
        t.equals(actual[0].type, 'put')
        t.equals(actual[0].name, '/a')
        t.equals(actual[0].nodes.length, 2)
        t.equals(actual[0].nodes[0].value, 'b')
        t.equals(actual[0].nodes[1].value, 'a')
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
