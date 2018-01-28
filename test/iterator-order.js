var tape = require('tape')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')
var hash = require('../lib/hash')

function sortByHash (a, b) {
  var ha = hash(typeof a === 'string' ? a : a.key, true).join('')
  var hb = hash(typeof b === 'string' ? b : b.key, true).join('')
  if (ha < hb) return -1
  if (ha > hb) return 1
  return 0
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
      cb => t.end()
    )
  })
})

function testSingleFeedWithKeys (t, keys, cb) {
  t.comment('with single feed')
  var db = create.one()
  put(db, keys, function (err) {
    t.error(err, 'no error')
    testIteratorOrder(t, db.iterator(), keys, cb)
  })
}

function testTwoFeedsWithKeys (t, keys, cb) {
  t.comment('with values split across two feeds')
  create.two(function (db1, db2, replicate) {
    var half = Math.floor(keys.length / 2)
    run(
      cb => put(db1, keys.slice(0, half), cb),
      cb => put(db2, keys.slice(half), cb),
      cb => replicate(cb),
      cb => testIteratorOrder(t, db1.iterator(), keys, cb),
      cb => testIteratorOrder(t, db2.iterator(), keys, cb),
      done
    )
  })
  function done () {
    if (!cb) t.end()
    else cb()
  }
}

function testIteratorOrder (t, iterator, expected, done) {
  var sorted = expected.slice(0).sort(sortByHash)
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
