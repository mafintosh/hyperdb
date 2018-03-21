var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')
var hash = require('../lib/hash')

function sortByHash (a, b) {
  var ha = hash(typeof a === 'string' ? a : a.key).join('')
  var hb = hash(typeof b === 'string' ? b : b.key).join('')
  return ha.localeCompare(hb)
}

const cases = {
  'simple': ['a', 'b', 'c'],
  'mixed depth from root': ['a/a', 'a/b', 'a/c', 'b', 'c'],
  '3 paths deep': ['a', 'a/a', 'a/b', 'a/c', 'a/a/a', 'a/a/b', 'a/a/c']
}

Object.keys(cases).forEach((key) => {
  tape('iterator is hash order sorted (' + key + ')', function (t) {
    var keysToTest = cases[key]
    run(
      cb => testSingleFeedWithKeys(t, keysToTest, true, cb),
      cb => testTwoFeedsWithKeys(t, keysToTest, true, cb),
      cb => testSingleFeedWithKeys(t, keysToTest, false, cb),
      cb => testTwoFeedsWithKeys(t, keysToTest, false, cb),
      cb => t.end()
    )
  })
})

tape('fully visit a folder before visiting the next one', function (t) {
  t.plan(12)
  var db = create.one()
  put(db, ['a', 'a/b', 'a/b/c', 'b/c', 'b/c/d'], function (err) {
    t.error(err, 'no error')
    var ite = db.iterator()

    ite.next(function loop (err, val) {
      t.error(err, 'no error')
      if (!val) return t.end()

      if (val.key[0] === 'b') {
        t.same(val.key, 'b/c')
        ite.next(function (err, val) {
          t.error(err, 'no error')
          t.same(val.key, 'b/c/d')
          ite.next(loop)
        })
      } else {
        t.same(val.key, 'a')
        ite.next(function (err, val) {
          t.error(err, 'no error')
          t.same(val.key, 'a/b')
          ite.next(function (err, val) {
            t.error(err, 'no error')
            t.same(val.key, 'a/b/c')
            ite.next(loop)
          })
        })
      }
    })
  })
})

tape('iterator sorted via latest does not return duplicates', (t) => {
  const expected = ['a', 'b', 'a/b']
  create.two(function (db1, db2, replicate) {
    run(
      cb => put(db1, ['a', 'a/b', 'b', 'b', 'a'], cb),
      replicate,
      cb => put(db2, ['a/b', 'b', 'a', 'a'], cb),
      replicate,
      cb => testIteratorOrder(t, db1.iterator({ latest: true }), expected, null, cb),
      cb => testIteratorOrder(t, db2.iterator({ latest: true }), expected, null, cb),
      t.end
    )
  })
})

tape('iterator sorted via latest with simple fork', (t) => {
  const expected2 = ['hi', '0', '1', '9', '8', '7', '6', '5', '4', '3', '2']
  const expected1 = ['0', '1', '9', '8', '7', '6', '5', '4', '3', '2']
  create.two(function (db1, db2, replicate) {
    run(
      cb => db1.put('0', '0', cb),
      cb => db2.put('2', '2', cb),
      cb => db2.put('3', '3', cb),
      cb => db2.put('4', '4', cb),
      cb => db2.put('5', '5', cb),
      cb => db2.put('6', '6', cb),
      cb => db2.put('7', '7', cb),
      cb => db2.put('8', '8', cb),
      cb => db2.put('9', '9', cb),
      replicate,
      cb => db1.put('1', '1a', cb),
      cb => db2.put('1', '1b', cb),
      replicate,
      cb => db1.put('0', '00', cb),
      replicate,
      cb => db2.put('hi', 'ho', cb),
      cb => testIteratorOrder(t, db1.iterator({ latest: true }), expected1, null, cb),
      cb => testIteratorOrder(t, db2.iterator({ latest: true }), expected2, null, cb),
      t.end
    )
  })
})

function testSingleFeedWithKeys (t, keys, latest, cb) {
  const sort = latest ? 'latest' : 'hash'
  t.comment('with single feed sorted by ' + sort)
  if (latest) t.comment('sorted by latest')
  var db = create.one()
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, db.iterator({ latest }), keys, sort, cb)
  })
}

function testTwoFeedsWithKeys (t, keys, latest, cb) {
  const sort = latest ? 'latest' : 'hash'
  t.comment('with values split across two feeds sorted by ' + sort)
  create.two(function (db1, db2, replicate) {
    var half = Math.floor(keys.length / 2)
    run(
      cb => put(db1, keys.slice(0, half), cb),
      replicate,
      cb => put(db2, keys.slice(half), cb),
      replicate,
      cb => testIteratorOrder(t, db1.iterator({ latest }), keys, sort, cb),
      cb => testIteratorOrder(t, db2.iterator({ latest }), keys, sort, cb),
      done
    )
  })
  function done () {
    if (!cb) t.end()
    else cb()
  }
}

function testIteratorOrder (t, iterator, expected, sort, done) {
  var sorted = expected.slice(0)
  if (sort === 'latest') sorted.reverse()
  else if (sort === 'hash') sorted.sort(sortByHash)

  each(iterator, onEach, onDone)

  function onEach (err, node) {
    t.error(err, 'no error')
    var key = node.key || node[0].key
    t.same(key, sorted.shift())
  }
  function onDone () {
    t.same(sorted.length, 0)
    if (done === undefined) t.end()
    else done()
  }
}

function each (ite, cb, done) {
  ite.next(function loop (err, node) {
    if (err) return cb(err)
    if (!node) return done()
    cb(null, node)
    ite.next(loop)
  })
}
