var events = require('events')

var lock = require('mutexify')
var bench = require('@andrewosh/nanobench')
var inherits = require('inherits')

var create = require('./create')

var _lock = lock()

module.exports = Benchmarker

// TODO: might just want to use process.nanotime
function convertToNs (time) {
  return 1e9 * time[0] + time[1]
}

function Benchmarker (tag, trials, cb) {
  if (!(this instanceof Benchmarker)) return new Benchmarker(tag, trials, cb)
  trials = trials || 1

  var self = this

  var hyperTimes = []
  var levelTimes = []
  var memHyperTimes = []
  var memLevelTimes = []

  var _remaining = trials * 2

  for (var i = 0; i < trials; i++) {
    _lock(function (release) {
      create(function (err, hyperdb, leveldb, memhyperdb, memdb) {
        if (err) throw err

        bench(tag + ' with hyperdb', function (b) {
          var benchDone = _wrapDone(b.done)
          b.done = function () {
            hyperTimes.push(convertToNs(b.time))
            benchDone()
          }
          return cb(b, hyperdb)
        })

        bench(tag + ' with leveldb', function (b) {
          var benchDone = _wrapDone(b.done)
          b.done = function () {
            levelTimes.push(convertToNs(b.time))
            benchDone()
            release()
          }
          return cb(b, leveldb)
        })

        bench(tag + ' with hyperdb in memory', function (b) {
          var benchDone = _wrapDone(b.done)
          b.done = function () {
            memHyperTimes.push(convertToNs(b.time))
            benchDone()
            release()
          }
          return cb(b, memhyperdb)
        })

        bench(tag + ' with leveldb in memory', function (b) {
          var benchDone = _wrapDone(b.done)
          b.done = function () {
            memLevelTimes.push(convertToNs(b.time))
            benchDone()
            release()
          }
          return cb(b, memdb)
        })
      })
    })
  }

  function _wrapDone (done) {
    return function () {
      if (--_remaining === 0) {
        self.emit('finish', [ hyperTimes, levelTimes, memHyperTimes, memLevelTimes ])
      }
      done()
    }
  }

  return this
}
inherits(Benchmarker, events.EventEmitter)
