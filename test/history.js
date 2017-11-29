var tape = require('tape')
var create = require('./helpers/create')
var replicate = require('./helpers/replicate')

tape('empty history', function (t) {
  var db = create.one()

  var expected = []

  var rs = db.createHistoryStream()
  collect(rs, function (err, actual) {
    t.error(err, 'no error')
    t.deepEqual(actual, expected, 'diff as expected')
    t.end()
  })
})

tape('single value', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createHistoryStream()
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 1)
      t.equals(actual[0].key, '/a')
      t.equals(actual[0].value, '2')
      t.end()
    })
  })
})

function collect (stream, cb) {
  var res = []
  stream.on('data', res.push.bind(res))
  stream.once('error', cb)
  stream.once('end', cb.bind(null, null, res))
}
