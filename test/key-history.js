var tape = require('tape')

var replicate = require('./helpers/replicate')
var create = require('./helpers/create')
var put = require('./helpers/put')
var run = require('./helpers/run')

tape('empty db', (t) => {
  var db = create.one()
  run(
    cb => testHistory(t, db, 'hello', [], cb),
    t.end
  )
}, { timeout: 1000 })

tape('single feed', (t) => {
  var db = create.one()
  run(
    cb => put(db, [
      { key: 'hello', value: 'welt' },
      { key: 'null', value: 'void' },
      { key: 'hello', value: 'world' }
    ], cb),
    cb => testHistory(t, db, 'hello', ['world', 'welt'], cb),
    cb => testHistory(t, db, 'null', ['void'], cb),
    t.end
  )
}, { timeout: 1000 })

tape('single feed (same value)', (t) => {
  var db = create.one()
  run(
    cb => put(db, [
      { key: 'hello', value: 'welt' },
      { key: 'hello', value: 'darkness' },
      { key: 'hello', value: 'world' }
    ], cb),
    cb => testHistory(t, db, 'hello', ['world', 'darkness', 'welt'], cb),
    t.end
  )
}, { timeout: 1000 })

tape('two feeds', (t) => {
  create.two((db1, db2, replicate) => {
    run(
      cb => put(db1, [
        { key: 'hello', value: 'welt' },
        { key: 'null', value: 'void' }
      ], cb),
      replicate,
      cb => put(db2, [
        { key: 'hello', value: 'world' }
      ], cb),
      replicate,
      cb => testHistory(t, db1, 'hello', ['world', 'welt'], cb),
      t.end
    )
  })
}, { timeout: 1000 })

tape('two feeds with conflict', (t) => {
  create.two((db1, db2, replicate) => {
    run(
      cb => put(db1, [
        { key: 'hello', value: 'welt' },
        { key: 'null', value: 'void' }
      ], cb),
      cb => put(db2, [
        { key: 'hello', value: 'world' }
      ], cb),
      replicate,
      cb => testHistory(t, db1, 'hello', [['world', 'welt']], cb),
      t.end
    )
  })
}, { timeout: 1000 })

tape('three feeds with conflict', (t) => {
  create.three((db1, db2, db3, replicateAll) => {
    run(
      cb => put(db1, [
        { key: 'hello', value: 'welt' },
        { key: 'null', value: 'void' }
      ], cb),
      cb => replicate(db1, db2, cb),
      cb => put(db2, [
        { key: 'hello', value: 'world' }
      ], cb),
      cb => replicate(db1, db2, cb),
      cb => put(db3, [
        { key: 'hello', value: 'again' }
      ], cb),
      replicateAll,
      cb => testHistory(t, db1, 'hello', [['world', 'again'], 'welt'], cb),
      t.end
    )
  })
}, { timeout: 1000 })

tape('three feeds with all conflicting', (t) => {
  create.three((db1, db2, db3, replicateAll) => {
    run(
      cb => put(db1, [
        { key: 'hello', value: 'welt' },
        { key: 'null', value: 'void' }
      ], cb),
      cb => put(db2, [
        { key: 'hello', value: 'world' }
      ], cb),
      cb => put(db3, [
        { key: 'hello', value: 'again' }
      ], cb),
      replicateAll,
      cb => testHistory(t, db1, 'hello', [['world', 'again', 'welt']], cb),
      t.end
    )
  })
}, { timeout: 1000 })

tape('three feeds (again)', (t) => {
  var toVersion = v => ({ key: 'version', value: v })
  create.three((db1, db2, db3, replicateAll) => {
    var len = 5
    var expected = []
    for (var i = 0; i < len * 3; i++) {
      expected.push(i.toString())
    }
    run(
      cb => put(db1, expected.slice(0, len).map(toVersion), cb),
      replicateAll,
      cb => put(db2, expected.slice(len, len * 2).map(toVersion), cb),
      replicateAll,
      cb => put(db3, expected.slice(len * 2).map(toVersion), cb),
      replicateAll,
      cb => testHistory(t, db1, 'version', expected.reverse(), cb),
      t.end
    )
  })
}, { timeout: 1000 })

function testHistory (t, db, key, expected, cb) {
  var results = expected.slice(0)
  var stream = db.createKeyHistoryStream(key)
  stream.on('data', (data) => {
    var expected = results.shift()
    t.notEqual(expected, undefined)
    if (!Array.isArray(expected)) expected = [expected]
    t.same(data.length, expected.length)
    expected.forEach((value, i) => {
      t.same(data[i].value, value)
    })
  })
  stream.on('end', () => {
    t.same(results.length, 0)
    cb()
  })
  stream.on('error', cb)
}
