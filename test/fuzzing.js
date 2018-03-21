var tape = require('tape')
var seed = require('seed-random')

var create = require('./helpers/create')
var run = require('./helpers/run')
var put = require('./helpers/put')

var ALPHABET = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']

run(
  fuzzRunner,
  function (err) {
    if (err) console.error('Fuzz testing errored:', err)
    else console.log('Fuzz testing completed with no error!')
  }
)

/**
 * Fuzzing tests can be specified using an options dictionary with this form:
 * (Defaults specified in the example below).
 *
 * {
 *   keys: 1000, // The total number of records.
 *   writers: 1, // The total number of writers.
 *   keyDepth: 1, // The maximum number of path components per key.
 *   dirs: 20, // The approximate number of directories that will be created.
 *   dirSize: 10, // The approximate number of keys at each prefix level.
 *   prefixSize: 2, // The size of each path component.
 *   conflicts: 100, // The approximate number of conflicting keys.
 *   replications: 10, // The approximate number of all-to-all replications/tests.
 *   valueSize: 20, // The size of each value.
 *   seed: 'hello' // The seed (for repeatable testing).
 * }
 */
function defaultOpts (opts) {
  return Object.assign({
    keys: 1000,
    writers: 1,
    keyDepth: 1,
    dirs: 20,
    dirSize: 10,
    prefixSize: 2,
    conflicts: 100,
    replications: 10,
    valueSize: 20,
    seed: 'hello'
  }, opts || {})
}

function test (probability, rand) {
  return rand() < probability
}

function sample (arr, count, rand, withReplacement) {
  if (count > arr.length && !withReplacement) throw Error('Invalid sampling arguments.')
  var result = []
  while(result.length != count) {
    var candidate = arr[Math.floor(rand() * arr.length)]
    if (withReplacement) result.push(candidate)
    else if (result.indexOf(candidate) === -1) result.push(candidate)
  }
  return result
}

function makeDatabases (opts, cb) {
  opts = defaultOpts(opts)
  create.many(opts.writers, function (err, dbs, replicateByIndex) {
    if (err) throw err
    return cb(dbs, replicateByIndex)
  })
}

function generateData (opts) {
  opts = defaultOpts(opts)
  
  var random = seed(opts.seed)

  var keysPerReplication = []
  var writesPerReplication = []
  var writers = new Array(opts.writers).fill(0).map((_, i) => i)

  // Generate the list of all keys that will be inserted.
  var stack = []
  for (var i = 0; i < opts.keys; i++) {
    var prefix = sample(ALPHABET, opts.prefixSize, random, true).join('')

    var shouldPushDir = test(opts.dirs / opts.keys, random)
      && stack.length < opts.keyDepth
    var shouldPopDir = test((opts.keys - opts.levelSize) / opts.keys, random)
    var shouldReplicate = test(opts.replications / opts.keys, random)

    if (shouldPushDir) stack.push(prefix)
    if (shouldPopDir) stack.pop()

    if (!keysPerReplication[0]) keysPerReplication.unshift([])

    keysPerReplication[0].push(stack.join('/') + prefix)
    if (shouldReplicate) keysPerReplication.unshift([])
  }

  // Generate the values for those keys (including possible conflicts).
  for (i = 0; i < keysPerReplication.length; i++) {
    var keyBatch = keysPerReplication[i]
    var writeBatch = []
    for (var j = 0; j < keyBatch.length; j++) {
      var singleWrite = { key: keyBatch[j], values: [] }
      var shouldConflict = test(opts.conflicts / opts.keys, random)
      var keyWriters = null
      if (shouldConflict) {
        var numConflicts = Math.floor(random() * opts.writers) + 1
        keyWriters = sample(writers, numConflicts, random, false)
      } else {
        keyWriters = sample(writers, 1, random, false)
      }
      for (var z = 0; z < keyWriters.length; z++) {
        var valueString = sample(ALPHABET, opts.valueSize, random, true).join('')
        singleWrite.values[keyWriters[z]] = valueString
      }
      writeBatch.push(singleWrite)
    }
    writesPerReplication.push(writeBatch)
  }

  return writesPerReplication
}

function validate (db, processedBatches, cb) {
  var expectedWrites = {}
  // Assuming the batches are insertion order.
  for (var i = 0; i < processedBatches.length; i++) {
    var writeBatch = processedBatches[i]
    for (var j = 0; j < writeBatch.length; j++) {
      var singleWrite = writeBatch[i]
      expectedWrites[singleWrite.key] = singleWrite.values.filter(_ => true)
    }
  }
  var allKeys = Object.keys(expectedWrites)

  tape(['validating after', processedBatches.length, 'replications'].join(''), function (t) {
    t.plan(allKeys.length + 1)

    var readStream = db.createReadStream('/')    
    readStream.on('end', function () {
      t.same(Object.keys(expectedWrites).length, 0)
      return cb()
    })
    readStream.on('error', cb)
    readStream.on('data', function (nodes) {
      if (!nodes) return
      var key = nodes[0].key
      var values = nodes.map(node => node.value)
      t.same(expectedWrites[key], values)
      delete expectedWrites[key]
    })
  })
}

function fuzzRunner (opts, cb) {
  makeDatabases(opts, function (dbs, replicateByIndex) {
    var writesPerReplication = generateData(opts)
    var ops = []
    for (var i = 0; i < writesPerReplication.length; i++) {
      var batch = writesPerReplication[i]
      var batchOps = []
      for (var j = 0; j < batch.length; j++) {
        var singleWrite = batch[j]
        for (var z = 0; z < singleWrite.values.length; z++) {
          var db = dbs[z]
          batchOps.push((function (k, v) { return cb => put(db, [{
            key: k,
            value: v
          }], cb) })(singleWrite.key, singleWrite.values[z]))
        }
      }
      ops.push(batchOps)
      // Intersperse replication/validation between write batches.
      ops.push([
        // Currently replicating between all databases at every replication point.
        cb => replicateByIndex(cb),
        cb => validate(dbs[0], writesPerReplication.slice(0, i), cb)
      ])
    }

    var finished = 0
    doRun()

    function doRun (err) {
      if (err) return cb(err)
      if (finished === ops.length) return cb(null)
      ops[finished].push(doRun)
      run.apply(null, ops[finished++])
    }
  })
}
