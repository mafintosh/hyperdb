var tape = require('tape')
var create = require('./helpers/create')

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

  db.checkout(function (err, co) {
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

tape('new value, twice', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a', value: '2' }
  ]

  db.checkout(function (err, co) {
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
    db.checkout(function (err, co) {
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

tape('updated value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'del', name: '/a/d/r', value: '1' },
    { type: 'put', name: '/a/d/r', value: '3' }
  ]

  db.put('/a/d/r', '1', function (err) {
    t.error(err, 'no error')
    db.checkout(function (err, co) {
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

// TODO: multiple feeds

function collect (stream, cb) {
  var res = []
  stream.on('data', res.push.bind(res))
  stream.once('error', cb)
  stream.once('end', cb.bind(null, null, res))
}
