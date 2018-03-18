var p = require('path')
var fs = require('fs')
var random = require('./helpers/random')
var bench = require('./helpers/bench')
var mkdirp = require('mkdirp')

var STATS_DIR = p.join(__dirname, 'stats')
var TRIALS = 5
var SPECS = [
  { numKeys: 1e1 },
  { numKeys: 1e2 },
  { numKeys: 1e3 },
  { numKeys: 1e4 },
  { numKeys: 1e5 },
]

var stats = []

function benchRunner (type, func) {
  SPECS.forEach(function (spec) {
    var tag = [type, spec.numKeys, 'documents'].join(' ')
    var benchmark = bench(tag, TRIALS, function (b, db) {
      var data = random.fullData(spec)
      b.start()
      func(data, b, db)
    })
    benchmark.on('finish', function (times) {
      stats.push({
        type: type,
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

function benchBatchInsertions (data, b, db) {
  benchRunner('batch write', function (data, b, db) {
    db.batch(data, function (err) {
      if (err) throw err
      b.end()
      b.done()
    })
  })
}

function benchSingleInsertions () {
  benchRunner('single write', function (data, b, db) {
    var counter = data.length - 1
    function _insert () {
      db.put(data[counter].key, data[counter].value, function (err) {
        if (err) throw err
        if (counter ===  0) {
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

function benchFullIteration () {
  
}

function multiply (x, y) { return x * y }

function run () {
  benchBatchInsertions()
  benchSingleInsertions()
}
run()

process.on('exit', function () {
  var csv = 'type,db,numKeys,'
  for (var i = 0; i < TRIALS; i++) {
    csv += 't' + i + ','
  }
  csv += '\n'
  for (var i = 0; i < stats.length; i++) {
    var stat = stats[i]
    csv += stat.spec.numKeys + ','
    var dbs = Object.keys(stat.timing)
    for (var j = 0; j < dbs.length; j++) {
      var db = dbs[j]
      csv += [stat.type, db, stat.numKeys].join(',')
      csv += stat.timing[db].join(',')
      csv += '\n'
    }
  }
  mkdirp.sync(STATS_DIR)
  var statFile = p.join(STATS_DIR, 'random.csv')
  fs.writeFileSync(statFile, csv, { valueEncoding: 'utf8' })
})
