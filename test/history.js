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

tape('multiple values', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/b/0', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, '/a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, '/b/0')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('multiple values: same key', function (t) {
  var db = create.one()

  db.put('/a', '2', function (err) {
    t.error(err, 'no error')
    db.put('/a', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, '/a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, '/a')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('2 feeds', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      b.put('/b', '12', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 3)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,0')
        t.end()
      })
    }
  })
})

tape('2 feeds: clock conflict', function (t) {
  create.two(function (a, b) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        replicate(a, b, function () {
          a.put('/a', 'c', function () {
            replicate(a, b, function () {
              validate()
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 4)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,0')
        t.equals(actual[3].feed + ',' + actual[3].seq, '0,2')
        t.end()
      })
    }
  })
})

tape('3 feeds', function (t) {
  create.three(function (a, b, c) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        c.put('/a', 'c', function () {
          replicate(a, b, function () {
            replicate(a, c, function () {
              validate()
            })
          })
        })
      })
    })

    function validate () {
      var rs = a.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 5)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,0')
        t.equals(actual[3].feed + ',' + actual[3].seq, '1,1')
        t.equals(actual[4].feed + ',' + actual[4].seq, '2,0')
        t.end()
      })
    }
  })
})

tape('3 feeds: clock conflict', function (t) {
  create.three(function (a, b, c) {
    a.put('/a', 'a', function () {
      b.put('/a', 'b', function () {
        replicate(a, b, function () {
          c.put('/a', 'c', function () {
            replicate(a, c, function () {
              c.put('/a', 'd', function () {
                validate()
              })
            })
          })
        })
      })
    })

    function validate () {
      var rs = c.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 6)
        t.equals(actual[0].feed + ',' + actual[0].seq, '0,0')  // a
        t.equals(actual[1].feed + ',' + actual[1].seq, '0,1')  // a
        t.equals(actual[2].feed + ',' + actual[2].seq, '1,0')  // c
        t.equals(actual[3].feed + ',' + actual[3].seq, '2,0')  // b
        t.equals(actual[4].feed + ',' + actual[4].seq, '2,1')  // b
        t.equals(actual[5].feed + ',' + actual[5].seq, '1,1')  // c
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
