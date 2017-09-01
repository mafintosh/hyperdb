var tape = require('tape')
var create = require('./helpers/create')

tape('new value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'put', name: '/a', value: '2' }
  ]

  db.checkout(function (err, co) {
    db.put('/a', '2', function (err) {
      t.error(err, 'no error')
      var rs = db.createDiffStream(co, '/a')
      collect(rs, function (err, actual) {
        t.deepEqual(actual, expected, 'diff as expected')
        t.end()
      })
    })
  })
})

tape('updated value', function (t) {
  var db = create.one()

  var expected = [
    { type: 'del', name: '/a/d/r', value: '1' },
    { type: 'put', name: '/a/d/r', value: '3' },
    { type: 'put', name: '/a', value: '2' }
  ]

  db.put('/a/d/r', '1', function (err) {
    t.error(err, 'no error')
    db.checkout(function (err, co) {
      t.error(err, 'no error')
      db.put('/a', '2', function (err) {
        t.error(err, 'no error')
        db.put('/a/d/r', '3', function (err) {
          t.error(err, 'no error')
          var rs = db.createDiffStream(co, '/a')
          collect(rs, function (err, actual) {
            t.deepEqual(actual, expected, 'diff as expected')
            t.end()
          })
        })
      })
    })
  })
})

function collect (stream, cb) {
  var res = []
  stream.on('data', res.push.bind(res))
  stream.once('error', cb)
  stream.once('end', cb.bind(null, null, res))
}
