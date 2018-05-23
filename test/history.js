var tape = require('tape')
var collect = require('stream-collector')
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

  db.put('a', '2', function (err) {
    t.error(err, 'no error')
    var rs = db.createHistoryStream()
    collect(rs, function (err, actual) {
      t.error(err, 'no error')
      t.equals(actual.length, 1)
      t.equals(actual[0].key, 'a')
      t.equals(actual[0].value, '2')
      t.end()
    })
  })
})

tape('multiple values', function (t) {
  var db = create.one()

  db.put('a', '2', function (err) {
    t.error(err, 'no error')
    db.put('b/0', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, 'a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, 'b/0')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('multiple values: same key', function (t) {
  var db = create.one()

  db.put('a', '2', function (err) {
    t.error(err, 'no error')
    db.put('a', 'boop', function (err) {
      t.error(err, 'no error')
      var rs = db.createHistoryStream()
      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 2)
        t.equals(actual[0].key, 'a')
        t.equals(actual[0].value, '2')
        t.equals(actual[1].key, 'a')
        t.equals(actual[1].value, 'boop')
        t.end()
      })
    })
  })
})

tape('2 feeds', function (t) {
  create.two(function (a, b) {
    a.put('a', 'a', function () {
      b.put('b', '12', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createHistoryStream()
      var bi = b.feeds.indexOf(b.local)
      var ai = bi === 0 ? 1 : 0

      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 3)
        t.equals(actual[0].feed, ai)
        t.equals(actual[0].seq, 1)
        t.equals(actual[1].feed, ai)
        t.equals(actual[1].seq, 2)
        t.equals(actual[2].feed, bi)
        t.equals(actual[2].seq, 1)
        t.end()
      })
    }
  })
})

tape('reverse', function (t) {
  create.two(function (a, b) {
    a.put('a', 'a', function () {
      b.put('b', '12', function () {
        replicate(a, b, validate)
      })
    })

    function validate () {
      var rs = b.createHistoryStream({reverse: true})
      var bi = b.feeds.indexOf(b.local)
      var ai = bi === 0 ? 1 : 0

      collect(rs, function (err, actual) {
        t.error(err, 'no error')
        t.equals(actual.length, 3)
        t.equals(actual[0].feed, bi)
        t.equals(actual[0].seq, 1)
        t.equals(actual[1].feed, ai)
        t.equals(actual[1].seq, 2)
        t.equals(actual[2].feed, ai)
        t.equals(actual[2].seq, 1)
        t.end()
      })
    }
  })
})
