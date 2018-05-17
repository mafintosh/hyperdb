var tape = require('tape')
var cmp = require('compare')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')
var hash = require('../lib/hash')

function sortByHash (a, b) {
  var ha = hash(typeof a === 'string' ? a : a.key).join('')
  var hb = hash(typeof b === 'string' ? b : b.key).join('')
  return cmp(ha, hb)
}

function reverseSortByHash (a, b) {
  return -1 * sortByHash(a, b)
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
      cb => testSingleFeedWithKeys(t, keysToTest, cb),
      cb => testTwoFeedsWithKeys(t, keysToTest, cb),
      cb => testSingleFeedWithKeys(t, keysToTest, { reverse: true, sort: reverseSortByHash }, cb),
      cb => testTwoFeedsWithKeys(t, keysToTest, { reverse: true, sort: reverseSortByHash }, cb),
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

function testSingleFeedWithKeys (t, keys, opts, cb) {
  if (typeof opts === 'function') return testSingleFeedWithKeys(t, keys, null, opts)
  opts = opts || {}

  var sortFunc = opts.sort || sortByHash

  t.comment('with single feed')
  var db = create.one()
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, db.iterator(opts), keys, sortFunc, cb)
  })
}

function testTwoFeedsWithKeys (t, keys, opts, cb) {
  if (typeof opts === 'function') return testTwoFeedsWithKeys(t, keys, null, opts)
  opts = opts || {}

  var sortFunc = opts.sort || sortByHash

  t.comment('with values split across two feeds')
  create.two(function (db1, db2, replicate) {
    var half = Math.floor(keys.length / 2)
    run(
      cb => put(db1, keys.slice(0, half), cb),
      cb => put(db2, keys.slice(half), cb),
      cb => replicate(cb),
      cb => testIteratorOrder(t, db1.iterator(opts), keys, sortFunc, cb),
      cb => testIteratorOrder(t, db2.iterator(opts), keys, sortFunc, cb),
      done
    )
  })
  function done () {
    if (!cb) t.end()
    else cb()
  }
}

function testIteratorOrder (t, iterator, expected, sortFunc, done) {
  var sorted = expected.slice(0).sort(sortFunc)
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
