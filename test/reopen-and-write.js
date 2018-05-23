var tape = require('tape')
var create = require('./helpers/create')
var run = require('./helpers/run')
var hyperdb = require('..')
var messages = require('../lib/messages')

tape('3 writers, re-open and write, re-open again', function (t) {
  create.three(function (a, b, c) {
    var reopened

    run(
      cb => a.put('foo', 'bar', cb),
      testUncorrupted,
      reopenDb,
      cb => reopened.put('foo2', 'bar2', cb),
      reopenDb,
      testInflateValue,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      t.end()
    }

    function testUncorrupted (cb) {
      t.equal(a._writers.length, 3, 'correct number of writers')
      cb()
    }

    function reopenDb (cb) {
      reopened = hyperdb(reuseStorage(a))
      reopened.ready(function (err) {
        t.error(err, 'no error')
        cb()
      })
    }

    function testInflateValue (cb) {
      t.equals(reopened.source.length, 5, 'correct length')
      reopened.source.get(4, function (err, data) {
        t.error(err, 'no error')
        var val = messages.Entry.decode(data)
        t.equal(val.inflate, 2, 'correct inflate for new entry')
        cb()
      })
    }
  })
})

function reuseStorage (db) {
  return function (name) {
    var match = name.match(/^source\/(.*)/)
    if (match) {
      name = match[1]
      if (name === 'secret_key') return db.source._storage.secretKey
      return db.source._storage[name]
    }
    match = name.match(/^peers\/([0-9a-f]+)\/(.*)/)
    if (match) {
      var hex = match[1]
      name = match[2]
      var peerWriter = db._writers.find(function (writer) {
        return writer && writer._feed.discoveryKey.toString('hex') === hex
      })
      if (!peerWriter) throw new Error('mismatch')
      var feed = peerWriter._feed
      if (name === 'secret_key') return feed._storage.secretKey
      return feed._storage[name]
    } else {
      throw new Error('mismatch')
    }
  }
}
