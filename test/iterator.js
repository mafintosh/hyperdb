var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')

tape('basic iteration', function (t) {
  var db = create.one()
  var vals = ['a', 'b', 'c']
  var expected = toMap(vals)

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator(), function (err, map) {
      t.error(err, 'no error')
      t.same(map, expected, 'iterated all values')
      t.end()
    })
  })
})

tape('iterate a big db', function (t) {
  var db = create.one()

  var vals = range(4000, '#')
  var expected = toMap(vals)

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator(), function (err, map) {
      t.error(err, 'no error')
      t.same(map, expected, 'iterated all values')
      t.end()
    })
  })
})

tape('prefix basic iteration', function (t) {
  var db = create.one()
  var vals = ['foo/a', 'foo/b', 'foo/c']
  var expected = toMap(vals)

  vals = vals.concat(['a', 'b', 'c'])

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator('foo'), function (err, map) {
      t.error(err, 'no error')
      t.same(map, expected, 'iterated all values')
      t.end()
    })
  })
})

tape('empty prefix iteration', function (t) {
  var db = create.one()
  var vals = ['foo/a', 'foo/b', 'foo/c']
  var expected = {}

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator('bar'), function (err, map) {
      t.error(err, 'no error')
      t.same(map, expected, 'iterated all values')
      t.end()
    })
  })
})

tape('prefix iterate a big db', function (t) {
  var db = create.one()

  var vals = range(4000, 'foo/#')
  var expected = toMap(vals)

  vals = vals.concat(range(4000, '#'))

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator('foo'), function (err, map) {
      t.error(err, 'no error')
      t.same(map, expected, 'iterated all values')
      t.end()
    })
  })
})

tape('non recursive iteration', function (t) {
  var db = create.one()

  var vals = [
    'a',
    'a/b/c/d',
    'a/c',
    'b',
    'b/b/c',
    'c/a',
    'c'
  ]

  put(db, vals, function (err) {
    t.error(err, 'no error')
    all(db.iterator({recursive: false}), function (err, map) {
      t.error(err, 'no error')
      var keys = Object.keys(map).map(k => k.split('/')[0])
      t.same(keys.sort(), ['a', 'b', 'c'], 'iterated all values')
      t.end()
    })
  })
})

function range (n, v) {
  // #0, #1, #2, ...
  return new Array(n).join('.').split('.').map((a, i) => v + i)
}

function toMap (list) {
  var map = {}
  for (var i = 0; i < list.length; i++) {
    map[list[i]] = list[i]
  }
  return map
}

function all (ite, cb) {
  var vals = {}

  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return cb(null, vals)
    vals[node.key] = node.value
    ite.next(loop)
  })
}
