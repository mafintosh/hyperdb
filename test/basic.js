var tape = require('tape')
var ram = require('random-access-memory')
var hyperdb = require('../')

tape('get, put and get', function (t) {
  var db = create()

  db.get('hello', function (_, val) {
    t.notOk(val, 'no value')
    db.put('hello', 'world', function (err) {
      t.error(err, 'no error')
      db.get('hello', function (err, val) {
        t.error(err, 'no error')
        t.same(val, 'world')
        t.end()
      })
    })
  })
})

tape('put, get, put and get', function (t) {
  var db = create()

  db.put('hello', 'a', function (err) {
    t.error(err, 'no error')
    db.get('hello', function (err, val) {
      t.error(err, 'no error')
      t.same(val, 'a')
      db.put('hello', 'b', function (err) {
        t.error(err, 'no error')
        db.get('hello', function (err, val) {
          t.error(err, 'no error')
          t.same(val, 'b')
          t.end()
        })
      })
    })
  })
})

tape('put twice, get', function (t) {
  var db = create()

  db.put('a', 'a', function (err) {
    t.error(err, 'no error')
    db.put('b', 'b', function (err) {
      t.error(err, 'no error')
      db.get('a', function (err, val) {
        t.error(err, 'no error')
        t.same(val, 'a')
        db.get('b', function (err, val) {
          t.error(err, 'no error')
          t.same(val, 'b')
          t.end()
        })
      })
    })
  })
})

function create () {
  return hyperdb(ram, {
    reduce: (a, b) => a,
    map: a => a.value.toString()
  })
}
