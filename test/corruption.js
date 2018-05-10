var tape = require('tape')
var create = require('./helpers/create')
var run = require('./helpers/run')
var hyperdb = require('..')
var messages = require('../lib/messages')

tape('feed with corrupted inflate generates error', function (t) {
  create.three(function (a, b, c) {
    var corrupted

    run(
      cb => a.put('foo', 'bar', cb),
      testUncorrupted,
      corruptInflateRecord,
      openCorruptedDb,
      done
    )

    function done (err) {
      t.error(err, 'no error')
      t.end()
    }

    function testUncorrupted (cb) {
      t.equal(a._writers.length, 3, 'uncorrupted length')
      cb()
    }

    function corruptInflateRecord (cb) {
      var index = 2
      a.source.get(index, function (err, data) {
        t.error(err, 'no error')
        var val = messages.Entry.decode(data)
        val.inflate = 0 // Introduce corruption
        var corruptData = messages.Entry.encode(val)
        var storage = a.source._storage
        storage.dataOffset(index, [], function (err, offset, size) {
          t.error(err, 'no error')
          storage.data.write(offset, corruptData, cb)
        })
      })
    }

    function openCorruptedDb (cb) {
      corrupted = hyperdb(reuseStorage(a))
      corrupted.ready(function (err) {
        t.ok(err, 'expected error')
        t.equal(err.message, 'Missing feed mappings', 'error message')
        t.equal(corrupted._writers.length, 2, 'corrupted length')
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
