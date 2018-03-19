var p = require('path')
var fs = require('fs')
var mkdirp = require('mkdirp')

var random = require('./helpers/random')
var bench = require('./helpers/bench')

var STATS_DIR = p.join(__dirname, 'stats')
var TRIALS = 3
var WRITE_SPECS = [
  { numKeys: 1e1 },
  { numKeys: 1e2 },
  { numKeys: 1e3 },
  { numKeys: 1e4 },
  { numKeys: 1e5 }
]
var READ_SPECS = [
  { numKeys: 1e1, numReads: 1000, readLength: 30, prefixLength: 1 },
  { numKeys: 1e2, numReads: 1000, readLength: 30, prefixLength: 1 },
  { numKeys: 1e3, numReads: 1000, readLength: 30, prefixLength: 1 },
  { numKeys: 1e4, numReads: 1000, readLength: 30, prefixLength: 1 },
  { numKeys: 1e5, numReads: 1000, readLength: 30, prefixLength: 1 },
  { numKeys: 1e1, numReads: 1000, readLength: 300, prefixLength: 1 },
  { numKeys: 1e2, numReads: 1000, readLength: 300, prefixLength: 1 },
  { numKeys: 1e3, numReads: 1000, readLength: 300, prefixLength: 1 },
  { numKeys: 1e4, numReads: 1000, readLength: 300, prefixLength: 1 },
  { numKeys: 1e5, numReads: 1000, readLength: 300, prefixLength: 1 },
  { numKeys: 1e1, numReads: 1000, readLength: 300, prefixLength: 2 },
  { numKeys: 1e2, numReads: 1000, readLength: 300, prefixLength: 2 },
  { numKeys: 1e3, numReads: 1000, readLength: 300, prefixLength: 2 },
  { numKeys: 1e4, numReads: 1000, readLength: 300, prefixLength: 2 },
  { numKeys: 1e5, numReads: 1000, readLength: 300, prefixLength: 2 }
]

var writeStats = []
var readStats = []

function makeTag (spec) {
  return Object.keys(spec).map(function (key) { return key + ': ' + spec[key] }).join(',')
}

function runner (specs, stats, tag, func) {
  specs.forEach(function (spec) {
    var benchmark = bench([tag, makeTag(spec)].join(' '), TRIALS, function (b, db) {
      var data = random.data(spec)
      b.start()
      func(spec, data, b, db)
    })
    benchmark.on('finish', function (times) {
      stats.push({
        type: tag,
        spec: spec,
        timing: {
          hyper: times[0],
          level: times[1],
          memHyper: times[2],
          memLevel: times[3]
        }
      })
    })
  })
}

function writeRunner (type, func) {
  return runner(WRITE_SPECS, writeStats, type, func)
}

function readRunner (type, func) {
  return runner(READ_SPECS, readStats, type, func)
}

function benchBatchInsertions (data, b, db) {
  writeRunner('batch write', function (spec, data, b, db) {
    db.batch(data, function (err) {
      if (err) throw err
      b.end()
      b.done()
    })
  })
}

function benchSingleInsertions () {
  writeRunner('single write', function (spec, data, b, db) {
    var counter = data.length - 1
    function _insert () {
      db.put(data[counter].key, data[counter].value, function (err) {
        if (err) throw err
        if (counter === 0) {
          b.end()
          b.done()
        } else {
          counter--
          process.nextTick(_insert)
        }
      })
    }
    b.start()
    _insert()
  })
}

function benchRandomReads () {
  readRunner('random reads', function (spec, data, b, db) {
    db.batch(data, function (err) {
      if (err) throw err
      var count = 0
      function reader () {
        _read(db, random.string(spec.prefixLength), spec.readLength, function () {
          if (count++ >= spec.numReads) {
            b.end()
            b.done()
          } else {
            setTimeout(reader, 0)
          }
        })
      }
      b.start()
      reader()
    })
  })

  function _read (db, prefix, length, cb) {
    var count = 0
    var stream = db.createReadStream(prefix)
    stream.on('data', function (d) {
      if (count++ === length) {
        stream.destroy()
        stream.on('close', function () {
          return cb()
        })
      }
    })
    stream.on('end', function () {
      return cb()
    })
  }
}

function run () {
  benchBatchInsertions()
  benchSingleInsertions()
  benchRandomReads()
}
run()

process.on('exit', function () {
  saveReadStats()
  saveWriteStats()

  function saveWriteStats () {
    var csv = 'type,db,numKeys,'
    for (var i = 0; i < TRIALS; i++) {
      csv += 't' + i + ','
    }
    csv += '\n'
    for (i = 0; i < writeStats.length; i++) {
      var stat = writeStats[i]
      var dbs = Object.keys(stat.timing)
      for (var j = 0; j < dbs.length; j++) {
        var db = dbs[j]
        csv += [stat.type, db, stat.spec.numKeys].join(',') + ','
        csv += stat.timing[db].join(',')
        csv += '\n'
      }
    }
    mkdirp.sync(STATS_DIR)
    var statFile = p.join(STATS_DIR, 'writes-random-data.csv')
    fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
  }
  function saveReadStats () {
    var csv = 'type,db,numKeys,numReads,readLength,prefixLength,'
    for (var i = 0; i < TRIALS; i++) {
      csv += 't' + i + ','
    }
    csv += '\n'
    for (i = 0; i < readStats.length; i++) {
      var stat = readStats[i]
      var dbs = Object.keys(stat.timing)
      for (var j = 0; j < dbs.length; j++) {
        var db = dbs[j]
        csv += [stat.type, db, stat.spec.numKeys, stat.spec.numReads,
          stat.spec.readLength, stat.spec.prefixLength].join(',') + ','
        csv += stat.timing[db].join(',')
        csv += '\n'
      }
    }
    mkdirp.sync(STATS_DIR)
    var statFile = p.join(STATS_DIR, 'reads-random-data.csv')
    fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
  }
})
